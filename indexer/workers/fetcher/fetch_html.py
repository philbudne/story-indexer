import argparse
import csv
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from scrapy.crawler import CrawlerProcess

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory
from indexer.workers.fetcher.batch_spider import BatchSpider

"""
App interface to launching a scrapy crawler on a set of batched stories
"""

logger = logging.getLogger(__name__)


class HTMLFetcher(App):
    date: str
    batch_index: int

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # fetch_date
        ap.add_argument(
            "--fetch-date",
            dest="fetch_date",
            help="Date (in YYYY-MM-DD) to fetch",
        )

        # sample_size
        ap.add_argument(
            "--batch-index",
            dest="batch_index",
            type=int,
            default=None,
            help="The index of the batch to work on fetching",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        logger.info(self.args)

        fetch_date = self.args.fetch_date
        if not fetch_date:
            logger.fatal("need fetch date")
            sys.exit(1)

        self.fetch_date = fetch_date

        batch_index = self.args.batch_index
        if batch_index is None:
            logger.fatal("need batch index")
            sys.exit(1)
        self.batch_index = batch_index

    def main_loop(self) -> None:
        process = CrawlerProcess()
        logger.info(f"Launching Batch Spider Process for Batch {self.batch_index}")
        process.crawl(
            BatchSpider, fetch_date=self.fetch_date, batch_index=self.batch_index
        )
        process.start()


if __name__ == "__main__":
    app = HTMLFetcher(
        "HTML_Fetcher",
        "Reads in a batch created by RSS_Batcher and launches a scrapy process to download html content",
    )
    app.main()
