"""
Read CSVs of articles from legacy system from S3,
fetch referenced objects from S3 mediacloud-downloads-backup,
pack into WARC files, and upload to B2.

Meant to be run from EC2.

With code from hist-queuer, hist-fetcher, archiver
"""

import csv
import datetime as dt
import gzip
import io
import logging
import os
import re
import socket
import sys
from typing import BinaryIO, Set

import boto3
from botocore.exceptions import ClientError

from indexer.app import run
from indexer.queuer import Queuer
from indexer.story import BaseStory, StoryFactory
from indexer.story_archive_writer import StoryArchiveWriter

logger = logging.getLogger(__name__)

DOWNLOADS_BUCKET = "mediacloud-downloads-backup"
DOWNLOADS_PREFIX = "downloads/"

Story = StoryFactory()
FQDN = socket.getfqdn()  # most likely internal or container!
HOSTNAME = socket.gethostname()  # for filenames

# regular expression to try to extract date from CSV file name:
DATE_RE = re.compile(r"(\d\d\d\d)[_-](\d\d)[_-](\d\d)")

PID = str(os.getpid())  # allow multiple processes


class HistPacker(Queuer):  # loops processing input files, with "tracker"
    APP_BLOBSTORE = "QUEUER"  # first choice for blobstore/keys
    HANDLE_GZIP = True  # just in case

    def process_args(self) -> None:
        self.archives = 0
        self.archive: StoryArchiveWriter | None = None
        self.stories = 0
        self.archive_story_limit = 5000
        self.prefix = ""

        # NOTE: not using indexer.blobstore.S3.s3_client:
        # 1. Historical archive is (currently) on S3
        # 2. This allows defaulting to default keys in ~/.aws/credentials
        for app in ["HIST", "QUEUER"]:
            region = os.environ.get(f"{app}_S3_REGION")
            access_key_id = os.environ.get(f"{app}_S3_ACCESS_KEY_ID")
            secret_access_key = os.environ.get(f"{app}_S3_SECRET_ACCESS_KEY")
            if region and access_key_id and secret_access_key:
                break

        # None values will check ~/.aws/credentials
        self.s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def check_output_queues(self) -> None:
        pass

    def process_file(self, fname: str, fobj: BinaryIO) -> None:
        """
        called for each input file with open binary/bytes I/O object
        """

        # try extracting date from file name to create fetch_date for RSSEntry.
        m = DATE_RE.search(fname)
        if not m:
            raise Exception(f"could not parse date from {fname}")

        fetch_date = "-".join(m.groups())
        self.prefix = f"pack-{fetch_date}"
        self.archive_date = fetch_date

        # The legacy system downloaded multiple copies of a story (if
        # seen in different RSS feeds in different sources?).  Looking
        # at 2023-11-04/05 CSV files, 49% of URLs in the file appear
        # more than once.  Since the new system files stories by final
        # URL, downloading multiple copies is a waste (subsequent
        # copies rejected as dups), so try to filter them out WITHIN a
        # single CSV file.
        self.urls_seen: Set[str] = set()

        for row in csv.DictReader(io.TextIOWrapper(fobj)):
            logger.debug("%r", row)
            self.process_row(fname, row)
        self.close_archive()

    def process_row(self, fname: str, row: dict) -> None:  # noqa: C901
        # typical columns:
        # collect_date,stories_id,media_id,downloads_id,feeds_id,[language,]url
        url = row.get("url")
        if not isinstance(url, str) or not url:
            self.incr_stories("bad-url", repr(url))
            return

        if url in self.urls_seen:
            self.incr_stories("dups", url)
            return

        if not self.check_story_url(url):
            return

        dlid = row.get("downloads_id")
        # let hist-fetcher quarantine if bad

        # convert to int: OK if missing or malformed
        try:
            feeds_id = int(row["feeds_id"])
        except (KeyError, ValueError):
            # XXX cannot count w/ incr_stories (only incremented once per story)
            feeds_id = None

        # convert to int: OK if missing or malformed
        try:
            media_id = int(row["media_id"])
        except (KeyError, ValueError):
            # XXX cannot count w/ incr_stories (only incremented once per story)
            media_id = None

        story = Story()
        with story.rss_entry() as rss:
            # store downloads_id (used to download HTML from S3)
            # as the "original" location, for use by hist-fetcher.py
            rss.link = dlid
            rss.fetch_date = self.archive_date
            rss.source_feed_id = feeds_id
            rss.source_source_id = media_id  # media is legacy name
            rss.via = fname

        collect_date = row.get("collect_date", None)
        with story.http_metadata() as hmd:
            hmd.final_url = url
            if collect_date:
                # convert original date/time (treat as UTC) to a
                # timestamp of the time the HTML was fetched,
                # to preserve this otherwise unused bit of information.

                if len(collect_date) < 20:
                    collect_date += ".0"  # ensure fraction present

                # fromisoformat wants EXACTLY six digits of fractional
                # seconds, but the CSV files omit trailing zeroes, so
                # use strptime.  Append UTC timezone offset to prevent
                # timestamp method from treating naive datetime as in
                # the local time zone.  date/time stuff and time zones
                # are always a pain, but somehow, this particular
                # corner of Python seems particularly painful.
                collect_dt = dt.datetime.strptime(
                    collect_date + " +00:00", "%Y-%m-%d %H:%M:%S.%f %z"
                )
                hmd.fetch_timestamp = collect_dt.timestamp()

        # NOTE! language was extracted from legacy database (where possible) due to
        # Phil's mistaken assumption that language detection was CPU intensive, but
        # that was due to py3lang/numpy/openblas dot operator defaulting to one
        # worker thread per CPU core, and the worker threads all spinning for work,
        # raising the load average.

        # When parser.py is run with OPENBLAS_NUM_THREADS=1, the cpu use is
        # negligable, and the work to extract the language, and to make it possible
        # to pass "override" data in to mcmetadata.extract was for naught.  And it's
        # better to assume that current tools are better than the old ones.

        # Nonetheless, since the data is available, put it in the Story.  It will
        # either be overwritten, or be available if the Story ends up being
        # quarantined by the parser.
        lang = row.get("language", None)
        if lang:
            with story.content_metadata() as cmd:
                cmd.language = cmd.full_language = lang

        ################ from hist-fetcher
        rss = story.rss_entry()
        hmd = story.http_metadata()

        dlid_str = rss.link
        if dlid_str is None or not dlid_str.isdigit():
            self.incr_stories("bad-dlid", dlid_str or "EMPTY")
            sys.exit(1)  # XXXXXXXXXXXXX

        # download id as int for version picker
        dlid = int(dlid_str)  # validated above

        s3path = DOWNLOADS_PREFIX + dlid_str

        # hist-queuer checked URL
        url = story.http_metadata().final_url or ""

        # need to have whole story in memory (for Story object),
        # so download to a memory-based file object and decompress
        with io.BytesIO() as bio:
            try:
                self.s3.download_fileobj(
                    Bucket=DOWNLOADS_BUCKET, Key=s3path, Fileobj=bio
                )
            except ClientError as exc:
                # Try to detect non-existent object,
                # let any other Exception cause retry/quarantine
                error = exc.response.get("Error")
                if error and error.get("Code") == "404":
                    self.incr_stories("not-found", url or str(dlid))
                    return
                raise

            # XXX inside try? quarantine on error?
            html = gzip.decompress(bio.getbuffer())

        if html.startswith(b"(redundant feed)"):
            self.incr_stories("redundant", url or str(dlid))
            return

        if not self.check_story_length(html, url):
            return  # counted and logged

        logger.info("%d %s: %d bytes", dlid, url, len(html))
        with story.raw_html() as raw:
            raw.html = html

        with hmd:
            hmd.response_code = 200  # for archiver
            if not hmd.fetch_timestamp:  # no timestamp from queuer
                try:
                    resp = self.s3.head_object(Bucket=DOWNLOADS_BUCKET, Key=s3path)
                    hmd.fetch_timestamp = resp["LastModified"].timestamp()
                except KeyboardInterrupt:
                    raise
                except Exception:
                    pass

        ################ end from hist-fetcher
        self.write_story(story)  # calls incr_story: to increment and log
        self.urls_seen.add(url)  # mark URL as seen

    def write_story(self, story: BaseStory) -> None:
        """
        write story to current WARC (opening if needed),
        if at per-warc limit, close WARC
        """
        if self.archive is None:
            # use fetch_date in archive name prefix (rawYYYY-MM-DD?)
            self.archives += 1
            self.archive = StoryArchiveWriter(
                prefix=self.prefix,
                hostname=f"{HOSTNAME}.{PID}",
                fqdn=FQDN,
                serial=self.archives,
                work_dir=".",
                # rw=True,
            )

        self.archive.write_story(story)
        self.stories += 1
        if self.stories >= self.archive_story_limit:
            self.close_archive()
            sys.exit(0)

    def close_archive(self) -> None:
        """
        finish current WARC file; upload to B2???
        """
        if self.archive:
            self.archive.finish()
            # name = self.archive.filename
            local_path = self.archive.full_path
            size = self.archive.size
            # timestamp = self.archive.timestamp

            logger.info(
                "wrote %d stories to %s (%s bytes)", self.stories, local_path, size
            )
            self.archive = None
            self.stories = 0

            # XXX try to upload!!!


if __name__ == "__main__":
    run(
        HistPacker,
        "hist-packer",
        "Read CSV of historical stories, pack into WARCs",
    )
