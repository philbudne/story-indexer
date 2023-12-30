# XXX send kiss of death when pika thread exits?!
# XXX send gauge stats for URLs/domains active?
# XXX remove slots when active==0 if no recent connect error
# XXX handle missing URL schema?
# XXX handle bad URLs: Quarantine??
"""
"Threaded Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the Global Interpreter Lock (GIL) means
that threads don't give greater concurrency than async/coroutines
(only one thread/task runs at a time), BUT PEP703 describes work in
progress to eliminate the GIL, over time, enabling the code to run on
multiple cores.

When a Story can't be fetched because of connect rate or concurrency
limits, the Story is queued to a "fast delay" queue to avoid book keeping
complexity (and having an API that allows not ACKing a message immediately).

In theory we have thirty minutes to ACK a message
before RabbitMQ has a fit (closes connection), so
holding on to Stories that can't be processed
immediately is POSSIBLE, *BUT* the current API acks
the message on return from process_message.

To delay ACK would need:
1. A way to disable automatic ACK (ie; return False)
2. Passing the delivery tag (or whole InputMessage - bleh) to process_story
3. An "ack" method on the StorySender object.

Handling retries (on connection errors) would either require
re-pickling the Story, or the InputMessage
"""

import argparse
import logging
import signal
import time
from types import FrameType
from typing import NamedTuple, Optional
from urllib.parse import SplitResult, urlsplit

import requests
from mcmetadata.urls import NON_NEWS_DOMAINS
from requests.exceptions import ConnectionError

from indexer.story import MAX_HTML, BaseStory
from indexer.worker import (
    CONSUMER_TIMEOUT_SECONDS,
    DEFAULT_EXCHANGE,
    MultiThreadStoryWorker,
    QuarantineException,
    Requeue,
    StorySender,
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

# time cache server as bad after a connection failure
CONN_RETRY_MINUTES = 10

# requests timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # for each read?

# HTTP parameters:
MAX_REDIRECTS = 30
AVG_REDIRECTS = 3

# get from common place (with rss-fetcher)
USER_AGENT = "mediacloud bot for open academic research (+https://mediacloud.org)"
HEADERS = {"User-Agent": USER_AGENT}

# HHTP response codes to retry:
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

# distinct counters for these HTTP response codes:
SEPARATE_COUNTS = set([403, 404, 429])

logger = logging.getLogger("fetcher")  # avoid __main__


def non_news_domain(fqdn: str) -> bool:
    for nnd in NON_NEWS_DOMAINS:
        if fqdn == nnd or fqdn.endswith("." + nnd):
            return True
    return False


def url_fqdn(url: str) -> str:
    """
    extract fully qualified domain name from url
    """
    # using urlsplit/SplitResult: parseurl calls spliturl and only
    # adds ";params" handling and this code only cares about netinfo
    surl = urlsplit(url, allow_fragments=False)
    hn = surl.hostname
    if not hn:
        raise ValueError("bad hostname")
    return hn.lower()


class Retry(Exception):
    """
    for explicit retries
    """


class FetchReturn(NamedTuple):
    resp: Optional[requests.Response]

    # only valid if resp is None:
    counter: str
    quarantine: bool


class Fetcher(MultiThreadStoryWorker):
    WORKER_THREADS_DEFAULT = 200  # equiv to 20 fetchers, with 10 active fetches

    # Just discard stories after connection errors:
    # NOTE: other requests.exceptions may be needed
    # but entire RequestException hierarchy includes bad URLs
    NO_QUARANTINE = (Retry, ConnectionError)

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""

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

        # Make sure cannot attempt a URL twice
        # using old status information:
        assert CONN_RETRY_MINUTES < self.RETRY_DELAY_MINUTES

        assert self.args
        self.scoreboard = ScoreBoard(
            self,
            self.workers,
            self.args.slot_requests,
            self.args.issue_interval,
            CONN_RETRY_MINUTES * 60,
        )

        # Unless the input RSS entries are well mixed (and this would
        # not be the case if the rss-fetcher queued RSS entries to us
        # directly), RSS entries for the same domain will travel in
        # packs/clumps/trains.  If the "fast" delay is too long, that
        # allows only one URL to be issued each time the train passes.
        # So set the "fast" delay JUST long enough so they come back
        # when intra-request issue interval has passed.
        self.set_requeue_delay_ms(1000 * self.args.issue_interval)

        # enable debug dump on SIGQUIT (CTRL-\)
        def quit_handler(sig: int, frame: Optional[FrameType]) -> None:
            if self.scoreboard:
                self.scoreboard.debug_info_nolock()  # XXX TEMP

        # used to work, now dumping core on return
        signal.signal(signal.SIGQUIT, quit_handler)

    def periodic(self) -> None:
        """
        called from main_loop
        """
        assert self.scoreboard
        with self.timer("status"):
            self.scoreboard.debug_info_nolock()

    def fetch(self, sess: requests.Session, fqdn: str, url: str) -> FetchReturn:
        """
        perform HTTP get, tracking redirects looking for non-news domains

        Returns FetchReturn NamedTuple for uniform handling of counts.
        """
        redirects = 0

        # loop following redirects
        while True:
            with self.timer("get"):  # time each HTTP get
                try:
                    resp = sess.get(
                        url,
                        allow_redirects=False,
                        headers=HEADERS,
                        timeout=(CONNECT_SECONDS, READ_SECONDS),
                        verify=False,  # raises connection rate
                    )
                except (
                    requests.exceptions.InvalidSchema,
                    requests.exceptions.MissingSchema,
                    requests.exceptions.InvalidURL,
                ):
                    # all other exceptions trigger retries in indexer.Worker
                    return FetchReturn(None, "badurl2", False)

            if not resp.is_redirect:
                # here with a non-redirect HTTP response:
                # it could be an HTTP error!
                return FetchReturn(resp, "SNH", False)

            # here with redirect:
            nextreq = resp.next  # PreparedRequest
            if nextreq and nextreq.url:
                # maybe just use PreparedRequest in loop?
                url = nextreq.url
            else:
                url = ""

            redirects += 1
            if redirects >= MAX_REDIRECTS:
                return FetchReturn(None, "maxredir", False)

            if not url or not isinstance(url, str):
                return FetchReturn(None, "badredir", False)
            try:
                fqdn = url_fqdn(url)
            except (TypeError, ValueError):
                return FetchReturn(None, "badredir2", False)

            # NOTE: adding a counter here would count each story fetch attempt more than once
            logger.info("redirect (%d) => %s", resp.status_code, url)

            if non_news_domain(fqdn):
                return FetchReturn(None, "non-news2", False)

        # end infinite redirect loop

    def count_story(self, status: str) -> None:
        """
        call exactly once for each story
        """
        self.incr("stories", labels=[("status", status)])

    def discard(self, counter: str, msg: str) -> None:
        """
        drop story
        """
        self.count_story(counter)
        logger.info("discard %s: %s", counter, msg)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called from multiple worker threads!!

        This routine should call count_story EXACTLY once!
        """
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return self.discard("no-url", "")
        assert isinstance(url, str)
        assert self.scoreboard is not None

        # XXX handle missing URL schema??

        # BEFORE issue (discard without any delay)
        try:
            fqdn = url_fqdn(url)
        except (TypeError, ValueError):
            return self.discard("badurl1", url)

        if non_news_domain(fqdn):
            return self.discard("non-news", url)

        # report time to issue: if this jumps up, it's
        # likely due to lock contention!
        with self.timer("issue"):
            ir = self.scoreboard.issue(fqdn, url)

        if ir.slot is None:  # could not be issued
            if ir.status == IssueStatus.SKIPPED:
                # Skipped due to recent connection error: Treat as if
                # we saw an error as well (incrementing retry count)
                # rather than waiting 30 seconds for connection to fail.
                logger.info("skipped %s", url)
                self.count_story("skipped")
                raise Retry("skipped due to recent connection failure")
            else:
                # here when "busy", due to one of (in order of likelihood):
                # 1. per-fqdn connect interval not reached
                # 2. per-fqdn currency limit reached
                # 3. total concurrecy limit reached.
                # requeue in short-delay queue, without counting as retry.
                logger.debug("busy %s", url)
                self.count_story("busy")
                raise Requeue("busy")  # does not increment retry count

        # here with slot marked active *MUST* call ir.slot.retire!!!!
        with self.timer("fetch"):
            # log starting URL
            logger.info("fetch %s", url)

            sess = requests.Session()
            try:  # call retire on exit
                fret = self.fetch(sess, fqdn, url)
                got_connection = True
            except Exception:
                self.count_story("noconn")
                got_connection = False
                raise
            finally:
                ir.slot.retire(got_connection)  # release slot
                sess.close()

        resp = fret.resp  # requests.Response
        if resp is None:  # NOTE!!! non-200 responses are Falsy?!
            if fret.quarantine:
                self.count_story(fret.counter)
                raise QuarantineException(fret.counter)
            return self.discard(fret.counter, url)

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

        # XXX report redirect count as a timing?

        logger.info("length %d", lcontent)  # XXX report ms?

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
