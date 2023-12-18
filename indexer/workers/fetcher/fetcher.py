"""
"Threaded Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the GIL (Global Interpreter Lock) means
that threads don't give greater concurrency than async (only one
thread/task runs at a time), BUT PEP703 describes work in progress to
eliminate the GIL, over time, enabling the code to run on multiple
cores.
"""

import argparse
import logging
import threading
import time
from typing import Optional
from urllib.parse import ParseResult, urlparse

import mcmetadata.urls
import requests

# PyPI
from pika.adapters.blocking_connection import BlockingChannel
from requests.exceptions import ChunkedEncodingError, ContentDecodingError
from requests.utils import to_native_string

from indexer.app import IntervalMixin
from indexer.story import BaseStory
from indexer.worker import (
    CONSUMER_TIMEOUT_SECONDS,
    QuarantineException,
    StorySender,
    Worker,
    run,
)
from indexer.workers.fetcher.sched import ScoreBoard

# _could_ try and map Slots by IP address(es), since THAT gets us closer
# to the point (of not hammering a particular server),
#
#   BUT: Would have to deal with:
#   1. A particular FQDN may map to multiple IP addrs
#   2. The order of the IP addreses might well change between queries
#   3. The ENTIRE SET might change if a CDN is involved!
#   4. Whether or not we're using IPv6 (if not, can ignore IPv6)
#   5. IP addresses can serve multiple domains
#   6. But domains in #5 might have disjoint IP addr sets.


# internal scheduling:
TOTAL_REQUESTS = 200  # equiv to 20 fetchers, with 10 active fetches
SLOT_REQUESTS = 2  # concurrent connections per domain
MIN_SECONDS = 1.0  # interval between issues for a domain
RETRY_SECONDS = 10 * MIN_SECONDS  # interval before re-issue after a failure

# TCP connection timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # fetch timeout

# HTTP parameters:
MAX_REDIRECTS = 30
AVG_REDIRECTS = 3

# RabbitMQ:
SHORT_DELAY = 5
PREFETCH_MULTIPLIER = 2  # two per thread (one active, one on deck)

# make sure not prefetching more than can be processed (worst case) per thread
assert (
    PREFETCH_MULTIPLIER * (CONNECT_SECONDS + READ_SECONDS) * AVG_REDIRECTS
    < CONSUMER_TIMEOUT_SECONDS
)

# get from common place (with rss-fetcher)
USER_AGENT = "mediacloud bot for open academic research (+https://mediacloud.org)"
HEADERS = {"User-Agent": USER_AGENT}

NON_NEWS_DOMAINS = set(mcmetadata.urls.NON_NEWS_DOMAINS)

RETRY_HTTP_CODES = set(
    [
        408,  # Request Timeout
        429,  # Too Many Requests
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
        # Cloudflare:
        522,  # Connection timed out
        524,  # A Timeout Occurred
    ]
)

SEPARATE_COUNTS = set([403, 404, 429])

# Possible polishing:
# Keep requests context in slot to reuse connections?
# Keep requests context per thread (using thread local storage)??

logger = logging.getLogger("fetcher")  # avoid __main__


class Retry(Exception):
    """
    for explicit retries
    """


class Fetcher(IntervalMixin, Worker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""
        self.total_requests = TOTAL_REQUESTS
        self.slot_requests = SLOT_REQUESTS
        self.thread_number = -1

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--slot-requests",
            "-S",
            type=int,
            default=SLOT_REQUESTS,
            help=f"requests/domain (default: {SLOT_REQUESTS})",
        )
        ap.add_argument(
            "--total-requests",
            "-T",
            type=int,
            default=TOTAL_REQUESTS,
            help=f"total active requests (default: {TOTAL_REQUESTS})",
        )
        ap.add_argument("input_file")

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit: distributes messages among workers
        processes, limits the number of unacked messages put into
        _message_queue
        """
        # single channel for all threads
        chan.basic_qos(prefetch_count=self.total_requests * PREFETCH_MULTIPLIER)

    def thread_body(self, thread_number: int) -> None:
        self.thread_number = thread_number
        # XXX catch exceptions??
        # XXX block signals?
        self._process_messages()

    def main_loop(self) -> None:
        assert self.args

        self.total_requests = self.args.total_requests
        self.slot_requests = self.args.slot_requests

        self.scoreboard = ScoreBoard(
            self.total_requests, self.slot_requests, MIN_SECONDS, RETRY_SECONDS
        )

        threads = []
        try:
            for i in range(0, self.total_requests):
                t = threading.Thread(
                    target=self.thread_body,
                    args=(i,),
                    name=f"worker {i}",
                )
                t.start()
                threads.append(t)

            while self._running:
                self.scoreboard.status()
                # XXX scan for dead threads?
                self.interval_sleep()
        finally:
            self._running = False
            # XXX may need to queue bogus work (a KOD) to wake up workers?
            # logger.info("waiting for workers")
            # threading.join(*threads)

    def count_story(self, status: str) -> None:
        """
        call exactly once for each story
        """
        self.incr("stories", labels=[("status", status)])

    def quarantine(self, reason: str) -> None:
        self.count_story(reason)
        raise QuarantineException(reason)

    def discard(self, url: str, reason: str) -> None:
        """
        drop story
        """
        self.count_story(reason)
        logger.debug("%s: discard %s", url, reason)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called from multiple worker threads
        """
        rss = story.rss_entry()

        # XXX use LogAdapter to prefix all msgs w/ URL, thread number???

        url = rss.link
        if not url:
            return self.quarantine("no-link")  # XXX discard?

        domain = rss.domain
        if not domain:
            return self.quarantine("no-domain")  # XXX discard?

        # XXX move inside redirect loop, and reissue against new domain??
        # (especially matters for news from aggregators)
        # if cannot issue for "next" domain, requeue with intermediate next???

        assert self.scoreboard is not None
        slot = self.scoreboard.issue(domain, url)
        if slot is None:  # cannot be issued
            # XXX requeue for fast retry, without counting as an error
            raise Retry("cannot issue")  # XXX TEMP!!!

        redirects = 0
        succ = False  # for retire

        # here with slot marked active
        try:  # call retire on exit
            logger.info("%s: starting %s", url, domain)

            # loop following redirects
            while True:
                if domain in NON_NEWS_DOMAINS:
                    return self.discard(url, "non-news")

                # let connection errors be handled by Worker class retries
                resp = requests.get(
                    url,
                    allow_redirects=False,
                    headers=HEADERS,
                    timeout=(CONNECT_SECONDS, READ_SECONDS),
                )
                if not resp.is_redirect:
                    break

                redirects += 1
                if redirects >= MAX_REDIRECTS:
                    return self.discard(url, "max-redirects")
                old_url = url
                parsed = self.handle_redirect(url, resp)
                url = parsed.geturl()
                logger.info("%s redirect (%d) to %s", old_url, resp.status_code, url)
                resp.close()
                # if getting new slot, extract fqdn!

            succ = True
        finally:
            slot.retire(succ)  # release slot

        status = resp.status_code
        if status != 200:
            if status in SEPARATE_COUNTS:
                counter = f"http-{status}"
            else:
                counter = f"http-{status//100}xx"

            if status in RETRY_HTTP_CODES:
                self.count_story(counter)
                reason = resp.reason
                logger.info("%s: retry %d %s", url, status, reason)
                raise Retry(f"HTTP {status} {reason}")
            else:
                return self.discard(url, counter)

        # here with status == 200; pack up and queue
        with story.http_metadata() as hmd:
            hmd.response_code = status
            hmd.final_url = resp.url
            hmd.encoding = resp.encoding
            hmd.fetch_timestamp = time.time()

        with story.raw_html() as rh:
            rh.html = resp.content  # bytes
            rh.encoding = resp.encoding

        sender.send_story(story)

    # XXX pass pre-parsed old url?
    def handle_redirect(self, url: str, resp: requests.Response) -> ParseResult:
        # UGH! code lifted from requests.Session
        try:
            resp.content  # Consume socket data so it can be released
        except (ChunkedEncodingError, ContentDecodingError, RuntimeError):
            resp.raw.read(decode_content=False)

        # Handle redirection without scheme (see: RFC 1808 Section 4)
        if url.startswith("//"):
            parsed_rurl = urlparse(resp.url)
            url = ":".join([to_native_string(parsed_rurl.scheme), url])

        # Normalize url case and attach previous fragment if needed (RFC 7231 7.1.2)
        parsed = urlparse(url)
        if parsed.fragment == "" and self.previous_fragment:
            parsed = parsed._replace(fragment=self.previous_fragment)
        elif parsed.fragment:
            self.previous_fragment = parsed.fragment
        return parsed


if __name__ == "__main__":
    run(Fetcher, "fetcher", "HTTP Page Fetcher")
