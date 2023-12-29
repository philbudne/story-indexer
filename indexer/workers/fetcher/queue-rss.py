"""
Standalone app to queue contents of MediaCloud synthetic RSS
file from rss-fetcher

fetches, un-gzips, parses XML, and queues on the fly (although
shuffling the full list, or interleaving larger feeds might help the
fetcher, and require reading it all before queuing anything).

NOTE! --yesterday only valid after about 00:30 GMT (19:30EST 20:30EDT)
"""

import argparse
import gzip
import html
import io
import logging
import sys
import time
import xml.sax
from typing import List, Optional

import requests

from indexer.app import App, run
from indexer.story import BaseStory, StoryFactory
from indexer.worker import StoryProducer, StorySender

Attrs = xml.sax.xmlreader.AttributesImpl

Story = StoryFactory()

logger = logging.getLogger("queue-rss")


class RSSHandler(xml.sax.ContentHandler):
    def __init__(self, app: "RSSQueuer", story_sender: Optional[StorySender]):
        self.app = app
        self.story_sender = story_sender
        self.parsed = 0
        self.in_item = False
        self.link = ""
        self.domain = ""
        self.pub_date = ""
        self.title = ""

    def startElement(self, name: str, attrs: Attrs) -> None:
        # error if in_item is True (missing end element?)
        if name == "item":
            self.in_item = True
        self.content: List[str] = []

    def characters(self, content: str) -> None:
        """
        text content inside current tag
        may come in multiple calls,
        even if/when no intervening element
        """
        # save channel.lastBuildDate for fetch date?
        self.content.append(content)

    def _count_story(self, status: str) -> None:
        self.app.incr("stories", labels=[("status", status)])

    def endElement(self, name: str):  # type: ignore[no-untyped-def]
        if not self.in_item:
            return

        content = "".join(self.content)
        # also pubDate, title
        if name == "link":
            # undo HTML entity escapes:
            self.link = html.unescape(content).strip()
        elif name == "domain":
            self.domain = content.strip()
        elif name == "pubDate":
            self.pub_date = content.strip()
        elif name == "title":
            self.title = content.strip()
        elif name == "item":
            if self.link and self.domain:
                s = Story()
                with s.rss_entry() as rss:
                    rss.link = self.link
                    rss.domain = self.domain
                    rss.pub_date = self.pub_date
                    rss.title = self.title
                    # also fetch_date, but not in rss file
                if self.story_sender:
                    # XXX maybe collect in batches of 1000 in a list
                    # and shuffle before queuing?  Would need to flush
                    # buffer after sax returns, and below when
                    # size-limit reached.
                    self._count_story("queued")
                    self.story_sender.send_story(s)
                    logger.info("queued %s", self.link)
                else:
                    # here for dry-run
                    logger.info("parsed %s", self.link)

                self.link = self.domain = self.pub_date = self.title = ""
                self.parsed += 1
                if (
                    self.app.sample_size is not None
                    and self.parsed == self.app.sample_size
                ):
                    logger.info("reached sample-size limit")
                    sys.exit(0)

            else:
                if self.story_sender:  # not dry-run?
                    self._count_story("bad")
                logger.warning(
                    "incomplete item: link: %s domain: %s", self.link, self.domain
                )
            self.in_item = False


class RSSQueuer(StoryProducer):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.sample_size: Optional[int] = None
        self.dry_run = False
        self.yesterday = time.strftime(
            "%Y-%m-%d", time.gmtime(time.time() - 24 * 60 * 60)
        )

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            default=False,
            help="don't queue stories",
        )

        ap.add_argument(
            "--sample-size",
            type=int,
            default=None,
            help="Number of stories to queue. Default (None) is 'all of them'",
        )

        # need exactly one of input_file, --fetch-date or --yesterday:
        group = ap.add_mutually_exclusive_group(required=True)

        group.add_argument("input_file", nargs="?", default=None)
        group.add_argument("--fetch-date", help="Date (in YYYY-MM-DD) to fetch")
        group.add_argument(
            "--yesterday",
            action="store_const",
            const=self.yesterday,
            dest="fetch_date",
        )

    def main_loop(self) -> None:
        assert self.args is not None

        # here with either input_file or fetch_date, but never both
        fname = self.args.input_file
        if not fname:
            fetch_date = self.args.fetch_date
            # basic date validation
            try:
                time.strptime(fetch_date, "%Y-%m-%d")  # validate
                if fetch_date > self.yesterday or fetch_date < "2022-02-18":
                    raise ValueError("date out of range")
            except ValueError:
                logger.error("bad fetch-date: %s", fetch_date)
                sys.exit(1)
            fname = f"https://mediacloud-public.s3.amazonaws.com/backup-daily-rss/mc-{fetch_date}.rss.gz"

        if fname.startswith("http:") or fname.startswith("https:"):
            logger.info("fetching %s", fname)
            resp = requests.get(fname, stream=True, timeout=60)
            if resp.status_code != 200:
                logger.error("could not fetch %s", fname)
                sys.exit(1)
            self.parse_rss(resp.raw, fname)
        else:
            with open(fname, "rb") as f:
                self.parse_rss(f, fname)

    def parse_rss(self, f: io.BufferedIOBase, fname: str) -> None:
        """
        parse possibly gzip'ed input stream
        """
        if fname.endswith(".gz"):
            self.parse_rss2(gzip.GzipFile(filename=fname, fileobj=f))
        else:
            self.parse_rss2(f)

    def parse_rss2(self, input: io.BufferedIOBase) -> None:
        """
        parse uncompressed input stream using RSSHandler (sends Stories)
        """

        assert self.args
        self.dry_run = self.args.dry_run
        self.sample_size = self.args.sample_size

        if self.dry_run:
            sender = None
        else:
            sender = self.story_sender()

        # XXX handle SAXParseException here, or pass error_handler=ErrorHandler
        xml.sax.parse(input, RSSHandler(self, sender))


if __name__ == "__main__":
    run(RSSQueuer, "queue-rss", "parse and queue rss-fetcher RSS entries")
