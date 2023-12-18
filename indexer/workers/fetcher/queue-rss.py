"""
Standalone app to queue contents of MediaCloud synthetic RSS
file from rss-fetcher

un-gzips and parses XML on the fly.

Currently takes ONLY local file path, but could take http URL,
and/or --yesterday, dates, etc
"""

import argparse
import gzip
import io
import logging
import xml.sax

from indexer.story import BaseStory, StoryFactory
from indexer.worker import StoryProducer, StorySender, run

Story = StoryFactory()

logger = logging.getLogger("queue-rss")


class RSSHandler(xml.sax.ContentHandler):
    def __init__(self, story_sender: StorySender):
        self.story_sender = story_sender
        self.in_item = False
        self.link = ""
        self.domain = ""

    def startElement(self, name, attrs) -> None:  # type: ignore[no-untyped-def]
        if name == "item":
            self.in_item = True
        self.tag = name

    def characters(self, content) -> None:  # type: ignore[no-untyped-def]
        """
        text content inside current tag
        """
        print("chars", self.tag, content)
        if self.in_item:
            # XXX strip+rstrip?
            if self.tag == "link":
                self.link = content
            elif self.tag == "domain":
                self.domain = content

    def endElement(self, name):  # type: ignore[no-untyped-def]
        if self.in_item and name == "item":
            if self.link and self.domain:
                s = Story()
                with s.rss_entry() as rss:
                    rss.link = self.link
                    rss.domain = self.domain
                self.story_sender.send_story(s)
                logger.info("queued %s", self.link)
                self.link = self.domain = ""
            else:
                logger.warning(
                    "incomplete item: link: %s domain: %s", self.link, self.domain
                )
            self.in_item = False


class RSSQueuer(StoryProducer):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument("input_file")

    def main_loop(self) -> None:
        assert self.args is not None

        fname = self.args.input_file
        # XXX check for --yesterday, etc
        if not fname:
            raise RuntimeError("no input_file")

        # code here to fetch file via HTTP if needed,
        # NOTE! requests Response.raw is an I/O stream!!

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
        parse uncompressed input stream
        """
        sender = self.story_sender()

        # XXX handle SAXParseException here, or pass error_handler=ErrorHandler
        xml.sax.parse(input, RSSHandler(sender))


if __name__ == "__main__":
    run(RSSQueuer, "queue-rss", "parse and queue rss-fetcher RSS entries")
