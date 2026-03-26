"""
SQLAlchemy table definitions

Using full ORM/Declarative declarations
"""

# PyPI:
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# declarative base class
class Base(DeclarativeBase):
    pass


# NOTE! column names currently match items in indexer.storyapp.PipeviewBreadcrumbV1!!
# DEPENDS on insert conflict, so the primary key is wide.


class Crumb(Base):
    """
    may need (many) additional indices to support query patterns,
    but that's left to alembic to manage.

    MUST have composite primary key OR constraint in order
    for UPDATE ... ON CONFLICT "incsert" to function properly!!
    """

    __tablename__ = "crumb"

    # YYYY-MM-DD:
    date: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)

    feed_id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, nullable=True
    )

    source_id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, nullable=True
    )

    # null before parser:
    domain: Mapped[str | None] = mapped_column(String, primary_key=True, nullable=True)

    app: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)

    status: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)

    # incremented for each matching breadcrumb:
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
