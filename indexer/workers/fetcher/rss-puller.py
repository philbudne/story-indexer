"""
StoryProducer that uses rss-fetcher API to pull stories
"""

# XXX read/write "last" file

import argparse
import datetime as dt
import email.utils
import logging
import os
import sys
import time
from typing import Tuple, TypedDict, cast

import requests

from indexer.app import AppException, run
from indexer.story import StoryFactory
from indexer.storyapp import StoryProducer, StorySender

Story = StoryFactory()


class StoryJSON(TypedDict):
    id: int
    url: str
    published_at: str | None
    domain: str
    title: str | None
    feed_id: int
    sources_id: int
    feed_url: str | None


logger = logging.getLogger("rss-puller")


def arg2env(name: str) -> str:
    return "RSS_FETCHER_" + name.upper()


class RSSFetcher(StoryProducer):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            default=False,
            help="don't queue stories",
        )

        def add(name: str) -> None:
            env = os.environ.get(arg2env(name))
            ap.add_argument(
                f"--{name}", default=env, help=f"rss-fetcher {name} (default: {env})"
            )

        add("server")
        add("user")
        add("password")

    def process_args(self) -> None:
        super().process_args()
        logger.info("HERE2")
        assert self.args
        self.dry_run = self.args.dry_run

        def get(name: str) -> str:
            val = getattr(self.args, name)
            if val == "" or val is None:
                logger.error("need --%s or %s env var", name, arg2env(name))
                sys.exit(1)
            assert isinstance(val, str)
            return val

        self.rss_fetcher_user = get("user")
        self.rss_fetcher_pass = get("password")
        self.rss_fetcher_server = get("server")
        self.rss_fetcher_url = f"http://{self.rss_fetcher_server}"

    def pull_stories(self, first: int, count: int) -> list[StoryJSON]:
        if self.rss_fetcher_user and self.rss_fetcher_pass:
            auth = requests.auth.HTTPBasicAuth(
                self.rss_fetcher_user, self.rss_fetcher_pass
            )
        else:
            auth = None

        hdrs = {"User-Agent": "story-indexer rss-puller"}
        url = f"{self.rss_fetcher_url}/api/rss_entries/{first}?_limit={count}"
        response = requests.get(url, auth=auth, headers=hdrs)

        if response.status_code != 200:
            raise AppException(f"status code {response.status_code}")

        j = response.json()
        status = j.get("status")
        if status != "OK":
            raise AppException(f"status {status}")

        return cast(list[StoryJSON], j.get("results"))

    def get_some(
        self, sender: StorySender, next: int, count: int
    ) -> Tuple[int, int | None]:
        """
        pull and queue stories
        """
        stories = self.pull_stories(next, count)
        got = len(stories)

        # print("start", next, "got", count)
        logger.error("start %d got %d", next, count)
        last = None

        for s in stories:
            # XXX check required fields present??
            last = s["id"]

            rfc2822_pub_date = None
            try:
                pub = s["published_at"]
                if pub is not None:
                    pub_dt = dt.datetime.fromisoformat(pub + "+00:00")
                    rfc2822_pub_date = email.utils.formatdate(pub_dt.timestamp())
            except (KeyError, TypeError, ValueError):
                pass

            story = Story()
            with story.rss_entry() as rss:
                rss.link = s["url"]
                rss.title = s.get("title")
                rss.domain = s["domain"]
                rss.pub_date = rfc2822_pub_date
                rss.fetch_date = s.get("fetched_at")
                rss.source_feed_id = s["feed_id"]
                rss.source_source_id = s["sources_id"]
                rss.source_url = s.get("feed_url")
                rss.via = self.rss_fetcher_url
            # will start Pika thread on first story

            if not self.dry_run:
                sender.send_story(story)
        # end for s in stories:
        return (got, last)

    def main_loop(self) -> None:
        logger.info("main loop")
        count = 1000  # XXX command line option?
        sender = self.story_sender()

        total = 0
        next = 0
        while True:
            # may sleep or exit if queues full enough
            self.check_output_queues()

            got, last = self.get_some(sender, next, count)
            total += got
            # XXX re-write "next" file (unless dry-run)

            if last is None or got < count:
                assert self.args
                if self.args.loop:
                    time.sleep(60)
                else:
                    logger.info("sent %d stories", total)
                    return
            assert last is not None
            next = last + 1

            # give rss-fetcher API a break
            # adds ~4 minutes to daily fetch of 500K stories
            time.sleep(0.5)


if __name__ == "__main__":
    # XXX TEMP: use rss-queuer-out exchange:
    run(RSSFetcher, "rss-queuer", "fetch stories using rss-fetcher API")
