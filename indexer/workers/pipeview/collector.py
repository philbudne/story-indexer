"""
Collect breadcrumbs from Story processing and sum in a PG database
"""

import argparse
import json
import logging
import os

# PyPI
import sqlalchemy.orm as orm
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import insert

# story-indexer/indexer
from indexer.app import AppException, run
from indexer.storyapp import StoryMixin
from indexer.worker import InputMessage, Worker

# local dir:
from indexer.workers.pipeview.models import Crumb

# Used to get columns for INSERT ... ON CONFLICT
CRUMB_TABLE = "crumb"
CRUMB_UNIQUE = "crumb_unique"

logger = logging.getLogger(__name__)


class Collector(Worker):  # NOT a StoryWorker!
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--database_url",
            default=os.environ.get("DATABASE_URL"),
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args

        database_url = self.args.database_url
        if not database_url:
            raise AppException("need DATABASE_URL")

        # pool_size? echo??
        self.engine = create_engine(database_url)

        # factory for SesionType with presupplied parameters:
        self.session_factory = orm.sessionmaker(bind=self.engine)

        inspector = inspect(self.engine)
        for index in inspector.get_indexes(CRUMB_TABLE):
            if index["name"] == CRUMB_UNIQUE and index["unique"]:
                self.unique_columns: list[str] = []
                # is typed list[str | None]:
                for c in index["column_names"]:
                    if isinstance(c, str):
                        self.unique_columns.append(c)
                assert len(self.unique_columns) > 0
                logger.info("crumb unique columns: %r", self.unique_columns)
                break
        else:
            raise AppException(f"could not find {CRUMB_TABLE} index {CRUMB_UNIQUE}")

    def process_message(self, im: InputMessage) -> None:
        """
        decode as newline separated JSON
        (easier to read and to recover from one bad entry)
        """

        lines = im.body.decode("utf-8").split("\n")
        logger.info("process_message: %d line(s)", len(lines))
        if not lines:
            # count? log??
            return

        rows: list[dict] = []
        first = True
        version = []
        for line in lines:
            try:
                j = json.loads(line)
                assert isinstance(j, dict)
            except Exception as e:
                # XXX count??
                logger.error("exception parsing %s: %.50r", line, e)
                continue

            if first:
                first = False
                if "version" in j:
                    version = j["version"]
                    if version[0] > StoryMixin.BREADCRUMB_VERSION[0]:
                        logger.info("breadcrumbs too new: %r", version)
                        return
                    continue

            logger.info("crumb: %r", j)
            rows.append(j)

        # XXX handle old version crumbs?
        # XXX counter!!

        with self.session_factory() as session:
            # XXX probably need to wrap in a try!!!
            session.begin()
            for row in rows:
                # an "upsert" that increments!
                # can't pass all the rows at once because
                # more than one may have same set of keys
                incsert_stmt = (
                    insert(Crumb)
                    .values(row)
                    .on_conflict_do_update(
                        index_elements=self.unique_columns,
                        set_={"count": Crumb.count + 1},
                    )
                )
                result = session.execute(incsert_stmt)
                logger.info("incsert result %r", result)
            session.commit()


if __name__ == "__main__":
    run(Collector, "pipeview", "pipeview collector")
