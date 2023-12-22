# XXX create LogAdapter, keep in tls????
# XXX empty HTML check!
# XXX align counter names w/ scrapy based fetcher!!!
# XXX split out WorkerThreads mixin?
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
    QuarantineException,
    StorySender,
    StoryWorker,
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
TOTAL_WORKERS = 200  # equiv to 20 fetchers, with 10 active fetches
SLOT_REQUESTS = 2  # concurrent connections per domain
MIN_SECONDS = 5.0  # interval between issues for a domain (5s = 12/min)

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
SHORT_DELAY_MS = int(MIN_SECONDS * 1000 + 100)
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


class Retry(Exception):
    """
    for explicit retries
    """


class Fetcher(IntervalMixin, StoryWorker):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""
        self.total_workers = TOTAL_WORKERS
        self.slot_requests = SLOT_REQUESTS

        self.fast_queue_name = fast_queue_name(process_name)

        # move to WorkerThreads mixin?
        self.threads: List[threading.Thread] = []
        self.tls = threading.local()  # thread local storage object
        self.tls.thread_number = -1

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
            "--total-workers",
            "-T",
            type=int,
            default=TOTAL_WORKERS,
            help=f"total active workers (default: {TOTAL_WORKERS})",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        self.total_workers = self.args.total_workers  # to WorkerThreads?
        self.slot_requests = self.args.slot_requests
        self.scoreboard = ScoreBoard(
            self.total_workers, self.slot_requests, MIN_SECONDS, CONN_RETRY_MINUTES * 60
        )

    # move to WorkerThreads mixin??
    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit: distributes messages among workers
        processes, limits the number of unacked messages put into
        _message_queue
        """
        # XXX maybe an increment rather than a multiplier??
        count = int(self.total_workers * PREFETCH_MULTIPLIER)
        chan.basic_qos(prefetch_count=count)

    # move to WorkerThreads mixin?
    def thread_body(self, thread_number: int) -> None:
        self.tls.thread_number = thread_number
        # XXX catch exceptions??
        # XXX block signals?
        self._process_messages()
        # XXX log @error? shutdown the whole process??

    # move to WorkerThreads mixin?
    def start_worker_threads(self, n: int) -> None:
        for i in range(0, self.total_workers):
            t = threading.Thread(
                target=self.thread_body,
                args=(i,),
                name=f"worker {i}",
            )
            t.start()
            self.threads.append(t)

    def periodic(self) -> None:
        if self.scoreboard:
            self.scoreboard.status()
        # XXX scan for dead threads?

    # move to WorkerThreads?
    def main_loop(self) -> None:
        try:
            self.start_worker_threads(self.total_workers)
            while self._running:
                self.periodic()
                self.interval_sleep()
        finally:
            self.queue_kisses_of_death(self.total_workers)
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
        called from multiple worker threads!!
        """
        rss = story.rss_entry()

        # XXX use LogAdapter to prefix all msgs w/ URL (or FQDN?), thread number???

        url = rss.link
        if not url:
            return self.quarantine("no-link")  # XXX discard?
        assert isinstance(url, str)
        assert self.scoreboard is not None

        # XXX move inside redirect loop, and reissue against new domain??
        # (especially matters for news from aggregators)
        # if cannot issue for "next" domain, requeue with intermediate next???
        fqdn = url_fqdn(url)
        ir = self.scoreboard.issue(fqdn, url)
        if ir.slot is None:  # could not be issued
            # skipped due to recent connection error,
            # treat as if we saw an error as well
            if ir.status == IssueStatus.SKIPPED:
                logger.info("skipping %s", url)
                # XXX counter?
                raise Retry("skipped")
            else:
                # here when "busy", one of:
                # 1. total concurrecy limit
                # 2. per-domain currency limit
                # 3. per-domain issue interval
                # requeue in short-delay queue, without counting as retry.
                # NOTE! using sender means story is re-pickled
                logger.info("busy %s", url)
                # XXX counter?
                sender.send_story(
                    story, DEFAULT_EXCHANGE, self.fast_queue_name, SHORT_DELAY_MS
                )
                return

        redirects = 0
        got_connection = False  # for retire

        # here with slot marked active
        reqsess = requests.Session()
        try:  # call retire on exit
            logger.info("starting %s", url)

            # loop following redirects
            while True:
                for nnd in NON_NEWS_DOMAINS:
                    if fqdn == nnd or fqdn.endswith("." + nnd):
                        return self.discard(url, "non-news")

                logger.info("getting %s", url)
                # let connection error exceptions cause retries
                got_connection = False
                resp = reqsess.get(
                    url,
                    allow_redirects=False,
                    headers=HEADERS,
                    timeout=(CONNECT_SECONDS, READ_SECONDS),
                )
                got_connection = True
                if not resp.is_redirect:
                    break

                old_url = url
                redirects += 1
                if redirects >= MAX_REDIRECTS:
                    return self.discard(url, "maxredir")
                nextreq = resp.next  # PreparedRequest
                if nextreq:
                    url = nextreq.url
                else:
                    url = ""
                if not url or not isinstance(url, str):
                    return self.discard(old_url, "badredir")
                fqdn = url_fqdn(url)
                logger.info("redirect (%d) %s to %s", resp.status_code, old_url, url)
                resp.close()
        finally:
            ir.slot.retire(got_connection)  # release slot
            reqsess.close()

        # XXX timing stat w/ redirect count?

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

        # here with status == 200
        content = resp.content  # bytes
        lcontent = len(content)

        # XXX check if empty

        if lcontent > MAX_HTML:
            return self.discard(url, "too-large")  # XXX name
        logger.info("%s ======== length %d", url, lcontent)

        with story.http_metadata() as hmd:
            hmd.response_code = status
            hmd.final_url = resp.url
            hmd.encoding = resp.encoding
            hmd.fetch_timestamp = time.time()

        with story.raw_html() as rh:
            rh.html = content
            rh.encoding = resp.encoding

        sender.send_story(story)


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


if __name__ == "__main__":
    run(Fetcher, "fetcher", "HTTP Page Fetcher")
