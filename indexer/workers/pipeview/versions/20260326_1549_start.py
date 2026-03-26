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
        sa.Column("date", sa.String, primary_key=True, nullable=False),
        sa.Column("feed_id", sa.Integer, primary_key=True),
        sa.Column("source_id", sa.Integer, primary_key=True),
        sa.Column("domain", sa.String, primary_key=True),
        sa.Column("app", sa.String, primary_key=True, nullable=False),
        sa.Column("status", sa.String, primary_key=True, nullable=False),
        sa.Column("count", sa.Integer, nullable=False, default=1),
    )


def downgrade() -> None:
    op.drop_table("crumb")
