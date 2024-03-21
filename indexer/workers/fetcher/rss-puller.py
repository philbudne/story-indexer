"""
StoryProducer that uses rss-fetcher API to pull stories
"""

import argparse
import datetime as dt
import email.utils
import logging
import os
import sys
import time
from typing import TypedDict, cast

import requests

from indexer.app import AppException, run
from indexer.path import app_data_dir
from indexer.story import RSSEntry, StoryFactory
from indexer.storyapp import StoryProducer, StorySender

Story = StoryFactory()


class StoryJSON(TypedDict):
    """
    JSON rows returned by rss-fetcher /api/rss_entries
    """

    id: int
    url: str
    published_at: str | None  # UTC in DB/ISO format w/o TZ
    domain: str | None
    title: str | None
    sources_id: int | None
    feed_id: int | None
    feed_url: str | None


class SerialFile:
    """
    read/write a file that keeps a numeric cookie

    Abstracted to a class so can be moved to an (ES?) database
    "properties" table

    MUST call last and write in pairs
    (don't assume last thing written is the contents of the file)
    """

    def __init__(self, path: str):
        self.path = path
        self.old_stats: os.stat_result | None = None

    def next(self) -> int:
        """
        return serial number of next item to read, or 0
        """
        try:
            with open(self.path) as f:
                next_ = int(f.readline().strip())
                self.old_stats = os.fstat(f.fileno())
        except FileNotFoundError:
            next_ = 0
            logger.info("%s not found: using %d", self.path, next)
            self.old_stats = None
        return next_

    def write(self, next_: int) -> None:
        """
        write cookie to file.
        Do NOT assume file will contain last written contents!!!
        """
        if self.old_stats:
            old_mtime = self.old_stats.st_mtime
            self.old_stats = None
            new_stat = os.stat(self.path)
            if new_stat.st_mtime != old_mtime:
                logger.warning("%s: modification time changed since read", self.path)
                # XXX maybe return??

        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(f"{next_}\n")
        # try to survive unremovable .prev file
        try:
            if os.path.exists(self.path):
                os.rename(self.path, self.path + ".prev")
        except OSError:
            pass
        os.rename(tmp, self.path)


logger = logging.getLogger("rss-puller")


def arg2env(name: str) -> str:
    return "RSS_FETCHER_" + name.upper()


class RSSFetcher(StoryProducer):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--count",
            default=1000,
            type=int,
            help="number of RSS entries to fetch in a batch",
        )

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

        add("url")
        add("user")
        add("password")

    def process_args(self) -> None:
        super().process_args()
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
        self.rss_fetcher_url = get("url")

    def pull_stories(self, first: int, count: int) -> tuple[list[StoryJSON], int]:
        url = f"{self.rss_fetcher_url}/api/rss_entries/{first}?_limit={count}"
        if self.rss_fetcher_user and self.rss_fetcher_pass:
            auth = requests.auth.HTTPBasicAuth(
                self.rss_fetcher_user, self.rss_fetcher_pass
            )
        else:
            auth = None
        hdrs = {"User-Agent": "story-indexer rss-puller"}
        response = requests.get(url, auth=auth, headers=hdrs)

        if response.status_code != 200:
            raise AppException(f"status code {response.status_code}")

        j = response.json()
        status = j.get("status")
        if status != "OK":
            raise AppException(f"status {status}")

        rows = cast(list[StoryJSON], j.get("results"))
        if len(rows) > 0:
            # this should be the ONE place that adds one.
            # include value in response JSON??
            next_ = rows[-1]["id"] + 1
        else:
            next_ = first
        return (rows, next_)

    def get_some(self, sender: StorySender, next_: int, count: int) -> tuple[int, int]:
        """
        pull and queue stories;
        returns count, new next_
        """
        stories, nnext = self.pull_stories(next_, count)
        got = len(stories)
        logger.info("next %d got %d", next_, got)

        for s in stories:
            id_ = s.get("id")
            url = s.get("url")

            if not isinstance(url, str):
                self.incr_stories("bad", str(url))  # log and count
                continue

            if not self.check_story_url(url):
                continue  # logged and counted

            rfc2822_pub_date = None
            try:
                pub = s["published_at"]
                if pub is not None:
                    pub_dt = dt.datetime.fromisoformat(pub + "+00:00")
                    rfc2822_pub_date = email.utils.formatdate(pub_dt.timestamp())
            except (KeyError, TypeError, ValueError):
                pass

            story = Story()
            with cast(RSSEntry, story.rss_entry()) as rss:  # XXX temp cast!
                rss.link = url
                rss.title = s.get("title")
                rss.domain = s.get("domain")
                rss.publication_date = rfc2822_pub_date
                rss.fetch_date = s.get("fetched_at")
                rss.source_feed_id = s.get("feed_id")
                rss.source_source_id = s.get("sources_id")
                rss.source_url = s.get("feed_url")
                rss.via = f"{id_}@{self.rss_fetcher_url}"
            # will start Pika thread on first story

            if not self.dry_run:
                sender.send_story(story)
        # end for s in stories:

        return (got, nnext)  # return new next

    def main_loop(self) -> None:
        assert self.args

        data_file = os.path.join(app_data_dir(self.process_name), "next-rss-entry")

        count = self.args.count
        sender = self.story_sender()

        total = 0
        serial_file = SerialFile(data_file)

        while True:
            # may sleep or exit if queues full enough
            self.check_output_queues()

            next_ = serial_file.next()
            got, new_next = self.get_some(sender, next_, count)
            total += got

            if new_next != next_:
                if not self.dry_run:
                    serial_file.write(new_next)

            if new_next == next_ or got < count:
                # got nothing or short batch
                assert self.args
                if self.args.loop:
                    time.sleep(60)
                else:
                    logger.info("quitting: sent %d stories", total)
                    return
            else:
                # give rss-fetcher API a quick break to avoid driving up load
                # 0.5 sec/k adds ~4 minutes to daily fetch of 500K stories
                time.sleep(0.5)


if __name__ == "__main__":
    run(RSSFetcher, "rss-puller", "pull stories using rss-fetcher API")
