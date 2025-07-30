"""add_timestamps

Revision ID: 3db278b3e383
Revises: 5fe462464963
Create Date: 2025-07-29 13:15:53.628525

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from sqlalchemy import text as sql_text

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3db278b3e383"
down_revision: Union[str, None] = "5fe462464963"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Att timestamp columns to existing tables
    op.add_column(
        "events",
        sa.Column("ts", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="alert",
    )
    op.add_column(
        "notices",
        sa.Column("ts", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="alert",
    )

    # Create the update_ts function to set the timestamp
    alert_update_ts = PGFunction(
        schema="alert",
        signature="update_ts()",
        definition="RETURNS TRIGGER\nLANGUAGE plpgsql AS\n$function$\nBEGIN\n    NEW.ts := now();\n    RETURN NEW;\nEND\n$function$",
    )
    op.create_entity(alert_update_ts)

    # Create triggers to call update_ts on update
    for table in ["events", "notices"]:
        update_ts_trigger = PGTrigger(
            schema="alert",
            signature=f"trig_update_ts_{table}",
            on_entity=f"alert.{table}",
            is_constraint=False,
            definition=f"BEFORE UPDATE ON alert.{table} FOR EACH ROW EXECUTE FUNCTION alert.update_ts()",
        )
        op.create_entity(update_ts_trigger)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop the triggers and functions
    for table in reversed(["events", "notices"]):
        op.drop_entity(
            PGTrigger(
                schema="alert",
                signature=f"trig_update_ts_{table}",
                on_entity=f"alert.{table}",
                is_constraint=False,
                definition="",  # definition is not needed for drop
            )
        )
    op.drop_entity(
        PGFunction(
            schema="alert",
            signature="update_ts()",
            definition="",  # definition is not needed for drop
        )
    )

    # Drop the timestamp columns
    op.drop_column("notices", "ts", schema="alert")
    op.drop_column("events", "ts", schema="alert")
