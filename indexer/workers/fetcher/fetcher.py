"""
"Threaded Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the GIL (Global Interpreter Lock) means
that threads don't give greater concurrency than async (only one
thread/task runs at a time), BUT PEP703 describes work in progress to
eliminate the GIL, over time, enabling the code to run on multiple
cores.

"""

import json
import logging
import math
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
from indexer.worker import CONSUMER_TIMEOUT_SECONDS, QuarantineException, Worker, run
from indexer.workers.nfetch.sched import ScoreBoard

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
MAX_ACTIVE = 200  # equiv to 20 fetchers, with 10 active fetches
MAX_PER_SLOT = 2  # concurrency
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

# Possible polishing:
# Keep requests context in slot to reuse connections?
# Keep requests context per thread (using thread local storage)??

logger = logging.getLogger("tfetcher")  # avoid __main__


class Fetcher(IntervalMixin, Worker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard = ScoreBoard(
            MAX_ACTIVE, MAX_PER_SLOT, MIN_SECONDS, RETRY_SECONDS
        )
        self.previous_fragment = ""

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit: distributes messages among workers
        processes, limits the number of unacked messages put into
        _message_queue
        """
        chan.basic_qos(prefetch_count=MAX_ACTIVE * PREFETCH_MULTIPLIER)

    def main_loop(self) -> None:
        threads = []
        try:
            for i in range(0, MAX_ACTIVE):
                t = threading.Thread(
                    # XXX supply wrapper (blocks signals? takes i)
                    target=self._process_messages,
                    name=f"worker {i}",
                )
                t.start()
                threads.append(t)

            while self._running:
                self.scoreboard.status()
                # XXX scan for dead threads?
                self.interval_sleep()
        finally:
            logger.info("waiting for workers")
            self._running = False
            # XXX may need to queue bogus work (a KOD) to wake up workers?
            # threading.join(*threads)

    def count_story(self, status: str) -> None:
        # one ring....
        self.incr("stories", labels=[("status", status)])

    def quarantine(self, reason: str) -> None:
        self.count_story(reason)
        raise QuarantineException(reason)

    def discard(self, reason: str) -> None:
        self.count_story(reason)
        # drop story

    def process_story(self, story: BaseStory) -> None:
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return self.quarantine("no-link")

        # using "canonical domain" (as the scrapy fetcher does)
        # alternative is FQDN from rss.link
        domain = rss.domain
        if not domain:
            return self.quarantine("no-domain")

        # XXX move inside redirect loop, and reissue against new domain??
        # (especially matters for news from aggregators)
        # if cannot issue for "next" domain, requeue with intermediate next???
        succ = False  # for retire
        slot = self.scoreboard.issue(domain, url)
        redirects = 0
        if slot is None:  # cannot be issued
            # XXX requeue with short delay
            return
        # here with slot marked active; MUST call slot.retire on exit!

        try:  # retire on exit
            logger.info("%s starting %s", domain, url)

            # loop following redirects
            while True:
                if domain in NON_NEWS_DOMAINS:
                    return self.discard("non-news")

                # retry on all exceptions, so let it go...
                resp = requests.get(
                    url,
                    allow_redirects=False,
                    headers=HEADERS,
                    timeout=(CONNECT_SECONDS, READ_SECONDS),
                )
                if resp.is_redirect:
                    redirects += 1
                    if redirects >= MAX_REDIRECTS:
                        return self.discard("max-redirects")
                    parsed = self.handle_redirect(url, resp)
                    url = parsed.geturl()
                    # XXX extract new domain (or what ever is being used as id)
                    logger.info("%s redirect (%d) to %s", domain, resp.status_code, url)
                    resp.close()
                    continue
                break  # not redirect

            succ = True
        finally:
            slot.retire(succ)  # release slot

        # XXX handle body here

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
