"""start

Revision ID: 7117bf03000d
Revises:
Create Date: 2026-03-26 15:49:57.486194

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7117bf03000d"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crumb",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("date", sa.String, nullable=False),
        sa.Column("feed_id", sa.BigInteger),
        sa.Column("source_id", sa.BigInteger),
        sa.Column("domain", sa.String),
        sa.Column("app", sa.String, nullable=False),
        sa.Column("status", sa.String, nullable=False),
        sa.Column("count", sa.BigInteger, nullable=False, server_default="1"),
    )
    # wanted to have a wide primary key and no id column,
    # but sqlalchemy made everything in primary-key not-null
    op.create_index(
        "crumb_unique",
        "crumb",
        ["date", "feed_id", "source_id", "domain", "app", "status"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("crumb_unique")
    op.drop_table("crumb")
