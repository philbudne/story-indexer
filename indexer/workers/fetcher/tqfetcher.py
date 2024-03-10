"""
"Threaded Queue Fetcher" using RabbitMQ

NOTE! As of 2023 with CPython, the Global Interpreter Lock (GIL) means
that threads don't give greater concurrency than async/coroutines
(only one thread/task runs at a time), BUT PEP703 describes work in
progress to eliminate the GIL, over time, enabling the code to run on
multiple cores.

Regardless, most of the time of an active fetch request is likely to
be waiting for I/O (due to network/server latency), or CPU bound in
SSL processing, neither of which requires holding the GIL.

We have thirty minutes to ACK a message before RabbitMQ has a fit
(closes connection), so:

* All scheduling done in Pika thread, as messages delivered by Pika
  * As messages come to _on_input_message, the next time a fetch could
    be issued is assigned by calling scoreboard.get_delay
  * If the delay would mean the fetch would start more than BUSY_DELAY_SECONDS
    in the future, the message is requeued to the "-fast" delay queue
    (and will return in BUSY_DELAY_SECONDS).
  * If connections to the server have failed "recently", behave as if
    this connection failed, and requeue the story for retry.
  * Else call pika_connection.call_later w/ the entire InputMessage and
    a callback to queue the InputMessage to the work queue (_message_queue)
    and the InputMessage will be picked up by a worker thread and passed
    to process_story()
"""

import argparse
import logging
import signal
import sys
import time
from types import FrameType
from typing import NamedTuple, Optional, Tuple

import requests
from mcmetadata.webpages import MEDIA_CLOUD_USER_AGENT
from pika.adapters.blocking_connection import BlockingChannel
from requests.exceptions import RequestException

from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import (
    MultiThreadStoryWorker,
    StorySender,
    non_news_fqdn,
    url_fqdn,
)
from indexer.worker import InputMessage, QuarantineException
from indexer.workers.fetcher.sched import DELAY_LONG, DELAY_SKIP, ConnStatus, ScoreBoard

TARGET_CONCURRENCY = 10  # scrapy fetcher AUTOTHROTTLE_TARGET_CONCURRENCY
MIN_INTERVAL_SECONDS = 1

# default delay time for "fast" queue, and max time to delay stories
BUSY_DELAY_SECONDS = 10 * 60

# time cache server as bad after a connection failure
CONN_RETRY_MINUTES = 10

# requests timeouts:
CONNECT_SECONDS = 30.0
READ_SECONDS = 30.0  # for each read?

# HTTP parameters:
MAX_REDIRECTS = 30

# scrapy default headers include: "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
HEADERS = {"User-Agent": MEDIA_CLOUD_USER_AGENT}

# HHTP response codes to retry
# (all others cause URL to be discarded)
RETRY_HTTP_CODES = set(
    [
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        # 501 is "Not Implemented"
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


class Retry(Exception):
    """
    Exception to throw for explicit retries
    (included in NO_QUARANTINE, so never quarantined)
    """


class FetchReturn(NamedTuple):
    resp: Optional[requests.Response]

    # only valid if resp is None:
    counter: str
    quarantine: bool


class Fetcher(MultiThreadStoryWorker):
    WORKER_THREADS_DEFAULT = 200  # equiv to 20 fetchers, with 10 active fetches

    # Exceptions to discard instead of quarantine after repeated retries:
    # RequestException hierarchy includes bad URLs
    NO_QUARANTINE = (Retry, RequestException)

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.scoreboard: Optional[ScoreBoard] = None
        self.previous_fragment = ""

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--busy-delay-seconds",
            type=float,
            default=BUSY_DELAY_SECONDS,
            help=f"busy (fast) queue delay in seconds (default: {BUSY_DELAY_SECONDS})",
        )

        ap.add_argument(
            "--conn-retry-minutes",
            type=float,
            default=CONN_RETRY_MINUTES,
            help=f"minutes to cache connection failure (default: {CONN_RETRY_MINUTES})",
        )

        ap.add_argument(
            "--target-concurrency",
            type=int,
            default=TARGET_CONCURRENCY,
            help=f"goal for concurrent requests/fqdn (default: {TARGET_CONCURRENCY})",
        )

        ap.add_argument(
            "--dump-slots",
            default=False,
            action="store_true",
            help="dump slot info once a minute",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args

        # Make sure cannot attempt a URL twice using old status information:
        if self.args.conn_retry_minutes >= self.RETRY_DELAY_MINUTES:
            logger.error(
                "conn-retry-minutes must be less than %d", self.RETRY_DELAY_MINUTES
            )
            sys.exit(1)

        self.busy_delay_seconds = self.args.busy_delay_seconds

        self.scoreboard = ScoreBoard(
            app=self,
            target_concurrency=self.args.target_concurrency,
            max_delay_seconds=self.busy_delay_seconds,
            conn_retry_seconds=self.args.conn_retry_minutes * 60,
            min_interval_seconds=MIN_INTERVAL_SECONDS,
        )

        self.set_requeue_delay_ms(1000 * self.busy_delay_seconds)

        # enable debug dump on SIGUSR1
        def usr1_handler(sig: int, frame: Optional[FrameType]) -> None:
            if self.scoreboard:
                self.scoreboard.debug_info_nolock()

        signal.signal(signal.SIGUSR1, usr1_handler)

    def _qos(self, chan: BlockingChannel) -> None:
        """
        set "prefetch" limit (number of unacked messages queued at any one time)
        """
        assert self.args
        prefetch = int(self.workers * self.busy_delay_seconds / MIN_INTERVAL_SECONDS)
        if prefetch > 65535:  # 16-bit field
            prefetch = 65535
        logger.info("setting prefetch to %d", prefetch)
        chan.basic_qos(prefetch_count=prefetch)

    def periodic(self) -> None:
        """
        called from main_loop
        """
        assert self.scoreboard
        assert self.args

        with self.timer("status"):
            self.scoreboard.periodic(self.args.dump_slots)

    def fetch(self, sess: requests.Session, url: str) -> FetchReturn:
        """
        perform HTTP get, tracking redirects looking for non-news domains

        Raises RequestException on connection and HTTP errors.
        Returns FetchReturn NamedTuple for uniform handling of counts.
        """
        redirects = 0

        # prepare initial request:
        request = requests.Request("GET", url, headers=HEADERS)
        prepreq = sess.prepare_request(request)
        while True:  # loop processing redirects
            with self.timer("get"):  # time each HTTP get
                # NOTE! maybe catch/retry malformed URLs from redirects??
                resp = sess.send(
                    prepreq,
                    allow_redirects=False,
                    timeout=(CONNECT_SECONDS, READ_SECONDS),
                    verify=False,  # raises connection rate
                )

            if not resp.is_redirect:
                # here with a non-redirect HTTP response:
                # it could be an HTTP error!

                # XXX report redirect count as a statsd "timing"? histogram??
                # with resp non-null, other args should be ignored
                return FetchReturn(resp, "SNH", False)

            # here with redirect:
            nextreq = resp.next  # PreparedRequest | None
            if nextreq:
                url = prepreq.url or ""
                prepreq = nextreq
            else:
                url = ""

            if not url:
                return FetchReturn(None, "badredir", False)

            redirects += 1
            if redirects >= MAX_REDIRECTS:
                return FetchReturn(None, "maxredir", False)

            try:
                fqdn = url_fqdn(url)
            except (TypeError, ValueError):
                return FetchReturn(None, "badredir2", False)

            # NOTE: adding a counter here would count each story fetch attempt more than once

            logger.info("redirect (%d) => %s", resp.status_code, url)
            if non_news_fqdn(fqdn):
                return FetchReturn(None, "non-news2", False)  # in redirect

        # end infinite redirect loop

    def get_id(self, story: BaseStory) -> Tuple[str, str, str]:
        """
        This function determines what stories are treated as from
        the same "server".

        Return tuple is: status/counter, url, ID
        XXX use a NamedTuple!!!

        NOT using "domain" from RSS file because I originally
        was planning to move the "issue" call inside the
        redirect loop (getting clearance for each FQDN along the
        chain), but if we ended up with a "busy", we'd have to
        retry and start ALL over, or add a field to the Story
        indicating the "next URL" to attempt to fetch, along
        with a count of followed redirects.  AND, using
        "canonical" domain means EVERYTHING inside a domain
        looks to be one server (when that may not be the case).

        *COULD* look up addresses, sort them, and pick the lowest or
        highest?!  this would avoid hitting single servers that handle
        many thing.dom.ain names hard, but incurrs overhead (and
        unless the id is stashed in the story object would require
        multiple DNS lookups: initial Pika thread dispatch, in worker
        thread for "start" call, and again for actual connection.
        Hopefully the result is cached nearby, but it would still incurr
        latency for due to system calls, network delay etc.
        """
        rss = story.rss_entry()

        url = rss.link
        if not url:
            return ("no-url", repr(url), "bad")

        assert isinstance(url, str)

        # BEFORE issue (discard without locking/delay)
        try:
            fqdn = url_fqdn(url)
        except (TypeError, ValueError):
            return ("badurl1", url, fqdn)

        if non_news_fqdn(fqdn):
            # unlikely, if queuer does their job!
            return ("non-news", url, fqdn)

        return ("ok", url, fqdn)

    def _on_input_message(self, im: InputMessage) -> None:
        """
        YIKES!! override a basic Worker method!!!
        Performs an additional decode of serialized Story!
        NOTE! Not covered by exception catching for retry!!!
        MUST ack and commit before returning!!!

        pre-processes incomming stories, delaying them
        (using the Pika "channel.call_later" method)
        so that they're queued to the worker pool
        with suitable inter-request delays for each server.

        DOES NOT INCREMENT STORY COUNTER!!!
        (perhaps have a different counter??)
        """
        assert self.scoreboard is not None
        assert self.connection is not None

        try:
            story = self.decode_story(im)

            status, url, id = self.get_id(story)
            if status != "ok":
                self.incr_stories(status, url)
                self._pika_ack_and_commit(im)  # drop (ack without requeuing)
                return

            with self.timer("get_delay"):
                delay = self.scoreboard.get_delay(id)

            logger.info("%s: delay %.3f", url, delay)

            if delay >= 0:

                def put() -> None:
                    # _put_message queue is the normal "_on_input_message" handler
                    self._put_message_queue(im)

                # round delay up to ms, to avoid calling early
                self.connection.call_later(round(delay, 3), put)
                # holding message, will be acked when processed
                return
            elif delay == DELAY_SKIP:
                raise Retry("skipped due to recent connection failure")
            elif delay == DELAY_LONG:
                self._requeue(im)
            else:
                raise Retry(f"unknown delay {delay}")
        except Exception as exc:
            self._retry(im, exc)
        self._pika_ack_and_commit(im)

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        called in a worker thread
        retry/quarantine exceptions handled normally
        """
        istatus, url, id = self.get_id(story)
        if istatus != "ok":
            logger.warning("get_id returned ('%s', '%s')", istatus, id)
            self.incr_stories(istatus, id)
            return

        assert self.scoreboard is not None
        slot = self.scoreboard.start(id, url)
        if slot is None:
            self.incr_stories("skipped2", url)
            raise Retry("skipped due to recent connection failure")

        # ***NOTE*** here with slot marked active *MUST* call slot.finish!!!!
        t0 = time.monotonic()
        with self.timer("fetch"):
            # log starting URL
            logger.info("fetch %s", url)

            sess = requests.Session()
            try:  # call retire on exit
                fret = self.fetch(sess, url)
                if fret.resp and fret.resp.status_code == 200:
                    conn_status = ConnStatus.DATA
                else:
                    conn_status = ConnStatus.NODATA
            except (
                requests.exceptions.InvalidSchema,
                requests.exceptions.MissingSchema,
                requests.exceptions.InvalidURL,
            ) as exc:
                logger.info("%s: %r", url, exc)
                self.incr_stories("badurl2", url)
                # bad URL, did not attempt connection, so don't mark domain as down!
                conn_status = ConnStatus.BADURL  # used in finally
                return  # discard: do not pass go, do not collect $200!
            except Exception:
                self.incr_stories("noconn", url)
                conn_status = ConnStatus.NOCONN  # used in finally
                raise  # re-raised for retry counting
            finally:
                # ALWAYS: report slot now idle!!
                # jumps in timing indicate lock contention!!
                with self.timer("finish"):
                    # keep track of connection success, latency
                    slot.finish(conn_status, time.monotonic() - t0)
                sess.close()

        resp = fret.resp  # requests.Response
        if resp is None:
            self.incr_stories(fret.counter, url)
            if fret.quarantine:
                raise QuarantineException(fret.counter)
            return

        status = resp.status_code
        if status != 200:
            if status in SEPARATE_COUNTS:
                counter = f"http-{status}"
            else:
                counter = f"http-{status//100}xx"

            msg = f"HTTP {status} {resp.reason}"
            if status in RETRY_HTTP_CODES:
                self.incr_stories(counter, url)
                raise Retry(msg)
            else:
                return self.incr_stories(counter, msg)

        # here with status == 200
        content = resp.content  # bytes
        lcontent = len(content)
        ct = resp.headers.get("content-type", "")

        logger.info("length %d content-type %s", lcontent, ct)

        # Scrapy skipped non-text documents: need to filter them out
        if not resp.encoding and not (
            ct.startswith("text/")
            or ct.startswith("application/xhtml")
            or ct.startswith("application/vnd.wap.xhtml+xml")
            or ct.startswith("application/xml")
        ):
            # other XML types handled by scrapy: application/atom+xml
            # application/rdf+xml application/rss+xml.
            # Logging the rejected content-types here
            # so that they can be seen in the log files:
            return self.incr_stories("not-text", url)

        if not self.check_story_length(content, url):
            return  # logged and counted

        final_url = resp.url
        with self.timer("queue"):
            with story.http_metadata() as hmd:
                hmd.response_code = status
                hmd.final_url = final_url
                hmd.encoding = resp.encoding  # from content-type header
                hmd.fetch_timestamp = time.time()

            with story.raw_html() as rh:
                rh.html = content
                rh.encoding = resp.encoding

            sender.send_story(story)
        self.incr_stories("success", final_url)


if __name__ == "__main__":
    run(Fetcher, "fetcher", "HTTP Page Fetcher")
