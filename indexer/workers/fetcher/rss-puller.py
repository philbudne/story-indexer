"""
Pull storeies using rss-fetcher API to fetch stories without an actual
RSS file.
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
from indexer.serialfile import SerialFile
from indexer.story import RSSEntry, StoryFactory
from indexer.storyapp import StoryProducer

Story = StoryFactory()

logger = logging.getLogger("rss-queuer")


# for API:
def rss_fetcher_name2opt(name: str) -> str:
    """
    convert short name to full option name
    """
    return "rss-fetcher-" + name


def rss_fetcher_name2var(name: str) -> str:
    """
    convert short name to name of args namespace property
    """
    return rss_fetcher_name2opt(name).replace("-", "_")


def rss_fetcher_name2env(name: str) -> str:
    return rss_fetcher_name2var(name).upper()


# for API:
class StoryJSON(TypedDict):
    """
    JSON rows returned by rss-fetcher API /api/rss_entries
    """

    id: int
    url: str
    published_at: str | None  # UTC in DB/ISO format w/o TZ
    domain: str | None
    title: str | None
    sources_id: int | None
    feed_id: int | None
    feed_url: str | None
    fetched_at: str | None  # added in rss-fetcher 0.16.1


class RSSPuller(StoryProducer):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # no defaults (all private)
        def add_rss_fetcher_arg(name: str) -> None:
            env = os.environ.get(rss_fetcher_name2env(name))
            ap.add_argument(
                "--" + rss_fetcher_name2opt(name),
                default=env,
                help=f"rss-fetcher API {name} (default: {env})",
            )

        add_rss_fetcher_arg("url")
        add_rss_fetcher_arg("user")
        add_rss_fetcher_arg("password")

        default_batch_size = int(os.environ.get("RSS_FETCHER_BATCH_SIZE", 1000))
        ap.add_argument(
            "--rss-fetcher-batch-size",
            type=int,
            default=default_batch_size,
            help=f"Use rss-fetcher API to fetch stories (default: {default_batch_size})",
        )

    def process_args(self) -> None:
        super().process_args()

        args = self.args
        assert args

        self.dry_run = args.dry_run

        def get(name: str) -> str:
            member_name = rss_fetcher_name2var(name)
            val = getattr(self.args, member_name)
            if val == "" or val is None:
                logger.error(
                    "need --%s or %s env var",
                    rss_fetcher_name2opt(name),
                    rss_fetcher_name2env(name),
                )
                sys.exit(1)
            assert isinstance(val, str)
            return val

        self.rss_fetcher_user = get("user")
        self.rss_fetcher_pass = get("password")
        self.rss_fetcher_url = get("url")

    def api_pull_stories(self, first: int, count: int) -> tuple[list[StoryJSON], int]:
        # There is an RSS API access class in the web-search repo, but it's
        # not a "public API", so there doesn't seem to be much point in putting
        # the code in a PyPI module of it's own.
        url = f"{self.rss_fetcher_url}/api/rss_entries/{first}?_limit={count}"
        if self.rss_fetcher_user and self.rss_fetcher_pass:
            auth = requests.auth.HTTPBasicAuth(
                self.rss_fetcher_user, self.rss_fetcher_pass
            )
        else:
            auth = None
        hdrs = {"User-Agent": "story-indexer rss-queuer.py"}
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

    # for API:
    def api_get_and_queue(self, next_: int, count: int) -> tuple[int, int]:
        """
        pull and queue stories;
        returns count, new next_
        """
        stories, new_next = self.api_pull_stories(next_, count)
        got = len(stories)
        logger.info("next %d got %d", next_, got)

        for s in stories:
            id_ = s.get("id")
            url = s.get("url")

            if not isinstance(url, str):
                # don't muddy the water if just a dry-run:
                if not self.dry_run:
                    self.incr_stories("no-url", str(url))  # log and count
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
            rss: RSSEntry = story.rss_entry()  # Temp typing?
            with rss:
                rss.link = url
                rss.title = s.get("title")
                rss.domain = s.get("domain")
                rss.pub_date = rfc2822_pub_date
                # for tracking/debug only:
                rss.source_feed_id = s.get("feed_id")
                rss.source_source_id = s.get("sources_id")
                rss.source_url = s.get("feed_url")
                rss.via = f"{id_}@{self.rss_fetcher_url}"
                rss.fetch_date = s.get("fetched_at")  # rss-fetcher 0.16.1
            self.send_story(story)
        # end for s in stories:
        return (got, new_next)

    def main_loop(self) -> None:
        assert self.args

        total = 0
        batch_size = self.args.rss_fetcher_batch_size
        serial_file = SerialFile(
            os.path.join(app_data_dir(self.process_name), "next-rss-entry")
        )

        while True:
            # may sleep or exit if queues full enough
            self.check_output_queues()

            next_ = serial_file.next()
            got, new_next = self.api_get_and_queue(next_, batch_size)
            total += got

            if new_next != next_:
                if not self.dry_run:
                    serial_file.write(new_next)

            if new_next == next_ or got < batch_size:
                # got nothing or short batch
                assert self.args
                if self.args.loop:
                    logger.debug("sleeping...")
                    time.sleep(60)
                else:
                    logger.info("quitting: sent %d stories", total)
                    return
            else:
                # give rss-fetcher API a quick break to avoid driving up load.
                # 0.5 sec/k adds ~4 minutes to daily fetch of 500K stories
                time.sleep(0.5)


if __name__ == "__main__":
    run(RSSPuller, "rss-puller", "pull stories using rss-fetcher API")
