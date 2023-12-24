"""
Standalone app to queue contents of MediaCloud synthetic RSS
file from rss-fetcher

un-gzips and parses XML on the fly.

Currently takes ONLY local file path, but could take http URL,
and/or --yesterday, dates, etc
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

from indexer.story import BaseStory, StoryFactory
from indexer.worker import StoryProducer, StorySender, run

Story = StoryFactory()

logger = logging.getLogger("queue-rss")


class RSSHandler(xml.sax.ContentHandler):
    def __init__(self, story_sender: Optional[StorySender]):
        self.story_sender = story_sender
        self.in_item = False
        self.link = ""
        self.domain = ""

    def startElement(self, name, attrs) -> None:  # type: ignore[no-untyped-def]
        if name == "item":
            self.in_item = True
        self.tag = name
        self.content: List[str] = []

    def characters(self, content) -> None:  # type: ignore[no-untyped-def]
        """
        text content inside current tag
        """
        # save channel.lastBuildDate for fetch date?
        self.content.append(content)

    def endElement(self, name):  # type: ignore[no-untyped-def]
        if not self.in_item:
            return

        content = "".join(self.content)
        # also pubDate, title
        if name == "link":
            self.link = html.unescape(content).strip()
        elif name == "domain":
            self.domain = content.strip()
        elif name == "item":
            if self.link and self.domain:
                s = Story()
                with s.rss_entry() as rss:
                    rss.link = self.link
                    rss.domain = self.domain
                    # also fetch_date, pub_date, title
                if self.story_sender:
                    logger.info("queued %s", self.link)
                    self.story_sender.send_story(s)
                else:
                    logger.info("parsed %s", self.link)

                self.link = self.domain = ""
            else:
                logger.warning(
                    "incomplete item: link: %s domain: %s", self.link, self.domain
                )
            self.in_item = False


class RSSQueuer(StoryProducer):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            default=False,
            help="don't queue stories",
        )

        # NOTE! rss-fetcher generates an RSS file for the previous UTC day
        # by about 00:30 UTC
        yesterday = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 24 * 60 * 60))

        # need exactly one:
        group = ap.add_mutually_exclusive_group(required=True)
        group.add_argument("--fetch-date", "-D", help="Date (in YYYY-MM-DD) to fetch")
        group.add_argument(
            "--yesterday",
            "-Y",
            action="store_const",
            const=yesterday,
            dest="fetch_date",
        )
        group.add_argument("input_file", nargs="?", default=None)

    def main_loop(self) -> None:
        assert self.args is not None

        fname = self.args.input_file
        if not fname:
            # XXX validate fetch_date?
            fname = f"https://mediacloud-public.s3.amazonaws.com/backup-daily-rss/mc-{self.args.fetch_date}.rss.gz"

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
        if self.args.dry_run:
            sender = None
        else:
            sender = self.story_sender()

        # XXX handle SAXParseException here, or pass error_handler=ErrorHandler
        xml.sax.parse(input, RSSHandler(sender))


if __name__ == "__main__":
    run(RSSQueuer, "queue-rss", "parse and queue rss-fetcher RSS entries")
