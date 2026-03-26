"""
Collect breadcrumbs from Story processing and sum in a PG database
"""

import argparse
import json
import logging
import os

# PyPI
import sqlalchemy.orm as orm

# local dir
from models import Crumb
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import insert

# story-indexer
from indexer.app import AppException, run
from indexer.storyapp import StoryMixin
from indexer.worker import InputMessage, Worker

# Used for INSERT ... ON CONFLICT
CRUMB_PK_COLUMNS = [key.name for key in inspect(Crumb).primary_key]

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
        engine = create_engine(database_url)

        # factory for SesionType with presupplied parameters:
        self.session_factory = orm.sessionmaker(bind=engine)

    def process_message(self, im: InputMessage) -> None:
        """
        decode as newline separated JSON
        (easier to read and to recover from one bad entry)
        """

        lines = im.body.decode("utf-8").split("\n")
        if not lines:
            # count? log??
            return

        try:
            v = json.loads(lines[0])
            if "version" in v:
                lines.pop(0)
                if v[0] > StoryMixin.BREADCRUMB_VERSION[0]:
                    # XXX count??
                    logger.info("bad version: %r", v)
                    return
        except Exception as e:
            # XXX count??
            logger.error("exception parsing %s line: %.50r", lines[0], e)
            return

        rows: list[dict] = []
        for line in lines:
            try:
                j = json.loads(line)
                assert isinstance(j, dict)
                # XXX cast to storyapp.PipeviewBreadcrumbV1??
                # adjust as needed for back-compat with version
                # (almost CERTAINLY overkill!!!)???

                # XXX preen data to weed out constraint violations?
                rows.append(j)
            except Exception as e:
                # XXX count??
                logger.error("exception parsing %s: %.50r", line, e)
                continue

        # XXX count total??
        print("got", len(rows), "rows")

        # an "upsert" that increments!
        incsert_stmt = (
            insert(Crumb)
            .values(rows)
            .on_conflict_do_update(
                index_elements=CRUMB_PK_COLUMNS, set_={"count": Crumb.count + 1}
            )
        )

        with self.session_factory() as session:
            # XXX probably need to wrap in a try!!!
            result = session.execute(incsert_stmt)
            print("result", result)


if __name__ == "__main__":
    run(Collector, "pipeview", "pipeview collector")
