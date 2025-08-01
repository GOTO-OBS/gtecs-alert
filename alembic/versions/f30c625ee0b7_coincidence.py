"""coincidence

Revision ID: f30c625ee0b7
Revises: 3db278b3e383
Create Date: 2025-08-01 11:15:23.432892

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic_utils.pg_trigger import PGTrigger
from sqlalchemy import text as sql_text

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f30c625ee0b7"
down_revision: Union[str, None] = "3db278b3e383"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create Coincidences table
    op.create_table(
        "coincidences",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="alert",
    )

    # Add trigger to update 'ts' column on Coincidences table
    #
    # NB: This is taken from a separate migration to avoid circular dependencies when creating the
    # Coincidences table:
    #  1) Alembic detects the new table needs to be created
    #  2) alembic-utils tries to simulate the trigger creation to see if it needs updating
    #  3) The trigger references alert.coincidences which doesn't exist yet
    #  4) The simulation fails
    #
    # To get the autogenerate to work, I excluded the coincidences table from the triggers
    # in database.py:
    # ```
    # if table.schema == SCHEMA and 'ts' in table.columns and table.name != 'coincidences'
    # ```
    # then:
    #   upgrade the database (alembic upgrade head)
    #   run autogenerate again to create this code
    #   copy it into this migration (commented out) and delete the second migration file
    #   downgrade the database (alembic downgrade -1)
    #   uncomment the code
    #   finally run the upgrade again (alembic upgrade head)
    update_ts_trigger = PGTrigger(
        schema="alert",
        signature="trig_update_ts_coincidences",
        on_entity="alert.coincidences",
        is_constraint=False,
        definition="BEFORE UPDATE ON alert.coincidences\n    FOR EACH ROW EXECUTE FUNCTION alert.update_ts()",
    )
    op.create_entity(update_ts_trigger)

    # Add foreign key to Events table
    op.add_column(
        "events",
        sa.Column("coincidence_id", sa.Integer(), nullable=True),
        schema="alert",
    )
    op.create_foreign_key(
        "events_coincidence_id_fkey",
        "events",
        "coincidences",
        ["coincidence_id"],
        ["id"],
        source_schema="alert",
        referent_schema="alert",
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Remove foreign key from Events table
    op.drop_constraint(
        "events_coincidence_id_fkey", "events", schema="alert", type_="foreignkey"
    )
    op.drop_column("events", "coincidence_id", schema="alert")

    # Drop the trigger
    op.drop_entity(
        PGTrigger(
            schema="alert",
            signature="trig_update_ts_coincidences",
            on_entity="alert.coincidences",
            is_constraint=False,
            definition="",  # definition is not needed for drop
        )
    )

    # Drop Coincidences table
    op.drop_table("coincidences", schema="alert")
