# XXX send kiss of death when pika thread exits?!

"""
"Threaded Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the Global Interpreter Lock (GIL) means
that threads don't give greater concurrency than async/coroutines
(only one thread/task runs at a time), BUT PEP703 describes work in
progress to eliminate the GIL, over time, enabling the code to run on
multiple cores.
"""

import argparse
import logging
import threading
import time
from typing import Any, List, Optional
from urllib.parse import ParseResult, urlparse

import requests
from mcmetadata.urls import NON_NEWS_DOMAINS

# PyPI
from pika.adapters.blocking_connection import BlockingChannel
from requests.exceptions import ChunkedEncodingError, ContentDecodingError
from requests.utils import to_native_string

from indexer.app import IntervalMixin
from indexer.story import MAX_HTML, BaseStory
from indexer.worker import (
    CONSUMER_TIMEOUT_SECONDS,
    DEFAULT_EXCHANGE,
    MultiThreadStoryWorker,
    QuarantineException,
    StorySender,
    fast_queue_name,
    run,
)
from indexer.workers.fetcher.sched import IssueStatus, ScoreBoard

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
SLOT_REQUESTS = 2  # concurrent connections per domain
DOMAIN_ISSUE_SECONDS = 5.0  # interval between issues for a domain (5s = 12/min)

# time to consider a server bad after a connection failure
# should NOT be larger than indexer.worker.RETRY_DELAY_MINUTES
# (or else can be rejected twice before retrying)
CONN_RETRY_MINUTES = 10

# requests timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # for each read?

# HTTP parameters:
MAX_REDIRECTS = 30
AVG_REDIRECTS = 3

# RabbitMQ
SHORT_DELAY_MS = int(DOMAIN_ISSUE_SECONDS * 1000 + 100)
PREFETCH_MULTIPLIER = 2  # two per thread (one active, one on deck)

# make sure not prefetching more than can be processed (worst case) per thread
assert (
    PREFETCH_MULTIPLIER * (CONNECT_SECONDS + READ_SECONDS) * AVG_REDIRECTS
    < CONSUMER_TIMEOUT_SECONDS
)

# get from common place (with rss-fetcher)
USER_AGENT = "mediacloud bot for open academic research (+https://mediacloud.org)"
HEADERS = {"User-Agent": USER_AGENT}

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
# Keep requests Session in slot to reuse connections??
# Keep requests Session per thread

logger = logging.getLogger("fetcher")  # avoid __main__


def url_fqdn(url: str) -> str:
    """
    extract fully qualified domain name from url
    """
    purl = urlparse(url)
    netloc = purl.netloc.lower()
    # _could_ strip leading "www." (www.com loses)?
    # BUT avoiding slow "canonical domain" extraction
    # (does DNS lookups, and can treat big.com/foo and big.com/bar
    # as different domains)
    if ":" in netloc:
        return purl.netloc.split(":", 1)[0]
    return netloc


class Retry(Exception):
    """
    for explicit retries
    """


class Fetcher(MultiThreadStoryWorker):
    WORKER_THREADS_DEFAULT = 200  # equiv to 20 fetchers, with 10 active fetches

    # discard messages after MAX_RETRIES
    RETRY_QUARANTINE = False

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""

        # maybe move to "FastRetryMixin"?
        self.fast_queue_name = fast_queue_name(process_name)

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--issue-interval",
            "-I",
            type=int,
            default=DOMAIN_ISSUE_SECONDS,
            help=f"domain request interval (default: {DOMAIN_ISSUE_SECONDS})",
        )
        ap.add_argument(
            "--slot-requests",
            "-S",
            type=int,
            default=SLOT_REQUESTS,
            help=f"requests/domain (default: {SLOT_REQUESTS})",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        self.scoreboard = ScoreBoard(
            self.workers,
            self.args.slot_requests,
            self.args.issue_interval,
            CONN_RETRY_MINUTES * 60,
        )

    def periodic(self) -> None:
        """
        called from main_loop
        """
        if self.scoreboard:
            # perhaps purge idle slots w/ last error more
            # than CONN_RETRY_MINUTES ago once an hour?
            self.scoreboard.status()

    def count_story(self, status: str) -> None:
        """
        call exactly once for each story
        """
        self.incr("stories", labels=[("status", status)])

    def quarantine(self, reason: str) -> None:
        self.count_story(reason)
        raise QuarantineException(reason)

    def discard(self, reason: str, msg: str) -> None:
        """
        drop story
        """
        self.count_story(reason)
        logger.info("discard %s: %s", reason, msg)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called from multiple worker threads!!
        """
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return self.discard("no-url", "")
        assert isinstance(url, str)
        assert self.scoreboard is not None

        # XXX move inside redirect loop, and reissue against new domain??
        # (especially matters for news from aggregators)
        # if cannot issue for "next" domain, requeue with intermediate next???
        fqdn = url_fqdn(url)
        with self.timer("issue"):
            ir = self.scoreboard.issue(fqdn, url)
        if ir.slot is None:  # could not be issued
            if ir.status == IssueStatus.SKIPPED:
                # skipped due to recent connection error,
                # treat as if we saw an error as well
                logger.info("skipped")
                self.count_story("skipped")
                # XXX counter?
                raise Retry("skipped due to recent connection failure")
            else:
                # here when "busy", one of:
                # 1. total concurrecy limit
                # 2. per-domain currency limit
                # 3. per-domain issue interval
                # requeue in short-delay queue, without counting as retry.
                logger.info("busy %s", url)
                self.count_story("busy")
                # NOTE! using sender means story is re-pickled
                sender.send_story(
                    story, DEFAULT_EXCHANGE, self.fast_queue_name, SHORT_DELAY_MS
                )
                return

        redirects = 0
        got_connection = False  # for retire

        with self.timer("fetch"):
            # log starting URL
            logger.info("fetch %s", url)

            # here with slot marked active
            sess = requests.Session()  # XXX keep in TLS?
            resp = None
            try:  # call retire on exit
                # loop following redirects
                while True:
                    for nnd in NON_NEWS_DOMAINS:
                        if fqdn == nnd or fqdn.endswith("." + nnd):
                            return self.discard("non-news", url)

                    # let connection error exceptions cause retries
                    got_connection = False
                    with self.timer("get"):
                        resp = sess.get(
                            url,
                            allow_redirects=False,
                            headers=HEADERS,
                            timeout=(CONNECT_SECONDS, READ_SECONDS),
                            verify=False,  # raises connection rate
                        )
                    got_connection = True
                    if not resp.is_redirect:
                        break

                    redirects += 1
                    if redirects >= MAX_REDIRECTS:
                        return self.discard("maxredir", url)
                    nextreq = resp.next  # PreparedRequest
                    if nextreq:
                        # maybe just use PreparedRequest in loop?
                        url = nextreq.url
                    else:
                        url = ""
                    if not url or not isinstance(url, str):
                        return self.discard("badredir", repr(url))
                    fqdn = url_fqdn(url)
                    # NOTE: adding a counter here would count each story fetch attempt more than once
                    logger.info("redirect (%d) => %s", resp.status_code, url)
            finally:
                ir.slot.retire(got_connection)  # release slot
                sess.close()
                if not got_connection:
                    # here on Exception from "get"
                    # want retry (return would discard)
                    self.count_story("noconn")

        # check resp is not None?
        status = resp.status_code
        if status != 200:
            if status in SEPARATE_COUNTS:
                counter = f"http-{status}"
            else:
                counter = f"http-{status//100}xx"

            msg = f"HTTP {status} {resp.reason}"
            if status in RETRY_HTTP_CODES:
                self.count_story(counter)
                raise Retry(msg)
            else:
                return self.discard(counter, msg)

        # here with status == 200
        content = resp.content  # bytes
        lcontent = len(content)

        # XXX timing stat w/ redirect count?

        logger.info("length %d", lcontent)

        if lcontent == 0:
            return self.discard("no-html", url)
        elif lcontent > MAX_HTML:
            return self.discard("oversized", url)

        with self.timer("queue"):
            with story.http_metadata() as hmd:
                hmd.response_code = status
                hmd.final_url = resp.url
                hmd.encoding = resp.encoding
                hmd.fetch_timestamp = time.time()

            with story.raw_html() as rh:
                rh.html = content
                rh.encoding = resp.encoding

            sender.send_story(story)
        self.count_story("success")


if __name__ == "__main__":
    run(Fetcher, "fetcher", "HTTP Page Fetcher")
