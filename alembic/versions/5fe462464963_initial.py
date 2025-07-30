"""initial

Revision ID: 5fe462464963
Revises:
Create Date: 2025-07-29 11:49:22.871886

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5fe462464963'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create alert schema if it doesn't exist
    op.execute("CREATE SCHEMA IF NOT EXISTS alert")

    # Create tables
    # Note the order of table creation is important due to foreign key constraints.

    # Events table
    op.create_table(
        'events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('type', sa.String(length=255), nullable=False),
        sa.Column('origin', sa.String(length=255), nullable=False),
        sa.Column('time', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='alert'
    )
    op.create_index(op.f('ix_alert_events_name'), 'events', ['name'], unique=True, schema='alert')
    op.create_index(op.f('ix_alert_events_type'), 'events', ['type'], unique=False, schema='alert')

    # Notices table
    op.create_table(
        'notices',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('ivorn', sa.String(length=255), nullable=False),
        sa.Column('received', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('payload', sa.LargeBinary(), nullable=False),
        sa.Column('skymap', sa.LargeBinary(), nullable=True),
        sa.Column('event_id', sa.Integer(), nullable=True),
        sa.Column('survey_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['event_id'], ['alert.events.id'], ),
        sa.ForeignKeyConstraint(['survey_id'], ['obs.surveys.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('ivorn'),
        schema='alert'
    )
    op.create_index(op.f('ix_alert_notices_received'), 'notices', ['received'], unique=False, schema='alert')


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order of creation

    # Notices table
    op.drop_index(op.f('ix_alert_notices_received'), table_name='notices', schema='alert')
    op.drop_table('notices', schema='alert')

    # Events table
    op.drop_index(op.f('ix_alert_events_type'), table_name='events', schema='alert')
    op.drop_index(op.f('ix_alert_events_name'), table_name='events', schema='alert')
    op.drop_table('events', schema='alert')

    # Finally, drop the alert schema
    op.execute("DROP SCHEMA IF EXISTS alert CASCADE")
