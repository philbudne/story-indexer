"""
SQLAlchemy table definitions

Using full ORM/Declarative declarations
"""

# PyPI:
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, mapped_column


# declarative base class
class Base(DeclarativeBase):
    pass


# NOTE! column names currently match items in indexer.storyapp.PipeviewBreadcrumbV1!!
# DEPENDS on insert conflict, so the primary key is wide.


class Crumb(Base):
    """
    may need (many) additional indices to support query patterns,
    but that's left to alembic to manage.

    MUST have composite unique index named "crumb_unique"
    for UPDATE ... ON CONFLICT "incsert" to function properly!!
    """

    __tablename__ = "crumb"

    id = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    # YYYY-MM-DD:
    date = mapped_column(sa.String, nullable=False)

    feed_id = mapped_column(sa.BigInteger)

    source_id = mapped_column(sa.BigInteger)

    # null before parser:
    domain = mapped_column(sa.String)

    app = mapped_column(sa.String, nullable=False)

    status = mapped_column(sa.String, nullable=False)

    # incremented for each matching breadcrumb:
    count = mapped_column(sa.BigInteger, nullable=False, server_default="1")
