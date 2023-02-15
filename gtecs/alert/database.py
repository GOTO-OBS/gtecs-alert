"""Alert database archive functions and ORM."""


from contextlib import contextmanager

from gtecs.common.database import get_session

from sqlalchemy import Column, DateTime, Integer, LargeBinary, String
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base

from . import params


@contextmanager
def open_session():
    """Create a session context manager connection to the database.

    All arguments passed to `get_session()` are taken from `gtecs.alert.params`.
    """
    session = get_session(
        user=params.DATABASE_USER,
        password=params.DATABASE_PASSWORD,
        host=params.DATABASE_HOST,
        echo=params.DATABASE_ECHO,
        pool_pre_ping=params.DATABASE_PRE_PING,
    )
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


Base = declarative_base()
Base.metadata.schema = 'alert'


class VOEvent(Base):
    """A VOEvent message.

    Parameters
    ----------
    ivorn : string
        The VOEvent IVORN (IVOA Resource Name).
    received : datetime.datetime
        The time the VOEvent was received.
    payload : bytes
        The VOEvent XML payload, stored as binary data.

    skymap : `gototile.skymap.Skymap`, optional
        The skymap associated with this VOEvent, if any.
        The skymap is stored in the database as binary data.

    Attributes
    ----------
    db_id : int
        primary database key
        only populated when the instance is added to the database

    """

    # Set corresponding SQL table name
    __tablename__ = 'voevents'

    # Primary key
    db_id = Column('id', Integer, primary_key=True)

    # Columns
    ivorn = Column(String(255), nullable=False, unique=True)
    received = Column(DateTime, nullable=False, index=True, server_default=func.now())
    payload = Column(LargeBinary, nullable=False)
    skymap = Column(LargeBinary, nullable=True)

    def __repr__(self):
        strings = ['ivorn={}'.format(self.ivorn),
                   'received={}'.format(self.received),
                   ]
        return 'VOEvent({})'.format(', '.join(strings))
