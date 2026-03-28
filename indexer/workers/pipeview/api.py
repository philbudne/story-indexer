"""
API server for pipeview database
"""

import logging

# PyPI:
import uvicorn
from fastapi import FastAPI, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import async_sessionmaker

from indexer.app import App

# local directory:
from indexer.workers.pipeview.models import Crumb

logger = logging.getLogger("pipeview-api")

# TEMP!!!! External port number for a dev indexer stack:
DATABASE_URL = "postgresql+psycopg://postgres:123454321@127.0.0.1:54340/pipeview"

app = FastAPI(
    title="PipeView",
    description="Look inside story-indexer pipeline",
)

# pool_size, echo??
async_engine = create_async_engine(DATABASE_URL)

# https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
AsyncSession = async_sessionmaker(async_engine, expire_on_commit=False)


@app.get("/sum/")
async def sum(
    cols: str,  # comma separated list of column names
    # filters
    source_id: int | None = Query(default=None),
    feed_id: int | None = Query(default=None),
    domain: str | None = Query(default=None),
    app: str | None = Query(default=None),
    status: str | None = Query(default=None),
    # pagination
    # skip: int = Query(default=0),
    # limit: int = Query(default=10),
) -> list[dict[str, int | str | None]]:
    # XXX validate column names using models.CRUMB_UNIQUE_KEYS??
    columns = [getattr(Crumb, c).label(c) for c in cols.split(",")]
    query = select(*columns, func.sum(Crumb.count).label("count"))

    # apply filters
    if source_id is not None:
        query = query.where(Crumb.source_id == source_id)
    if feed_id is not None:
        query = query.where(Crumb.feed_id == feed_id)
    if domain is not None:
        query = query.where(Crumb.domain == domain)
    if app is not None:
        query = query.where(Crumb.app == app)
    if status is not None:
        query = query.where(Crumb.status == status)

    query = query.group_by(*columns)
    async with AsyncSession() as session:
        results = await session.execute(query)
        # row is sqlalchemy.engine.row.Row
        return [row._asdict() for row in results]


class PipeViewAPI(App):
    """
    Indexer app: initializes logging, stats
    """

    # XXX take args for host & port, default from environment??

    def main_loop(self) -> None:
        uvicorn.run(app, host="0.0.0.0", port=8888)


if __name__ == "__main__":
    from indexer.app import run

    run(PipeViewAPI, "pipeview-api", "PipeView API Server")
