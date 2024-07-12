"""Alert database archive functions and ORM."""

import datetime
import os
from contextlib import contextmanager

from astropy.time import Time

from gtecs.common.database import get_session as get_session_common
from gtecs.obs.database.models import Base

from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy import func
from sqlalchemy.orm import backref, relationship, validates

from . import params
from .notices import Notice as EventNotice
from .skymap import skymap_from_bytes


def get_session(user=None, password=None, host=None, echo=None, pool_pre_ping=None):
    """Create a database connection session.

    All arguments are passed to `gtecs.common.database.get_session()`,
    with the defaults taken from the module parameters.

    Note it is generally better to use the session_manager() context manager,
    which will automatically commit or rollback changes when done.
    """
    # This means the user doesn't need to worry about the params, but can overwrite if needed.
    if user is None:
        user = params.DATABASE_USER
    if password is None:
        password = params.DATABASE_PASSWORD
    if host is None:
        host = params.DATABASE_HOST
    if echo is None:
        echo = params.DATABASE_ECHO
    if pool_pre_ping is None:
        pool_pre_ping = params.DATABASE_PRE_PING
    session = get_session_common(
        user=user,
        password=password,
        host=host,
        echo=echo,
        pool_pre_ping=pool_pre_ping,
    )
    return session


@contextmanager
def session_manager(**kwargs):
    """Create a session context manager connection to the database."""
    session = get_session(**kwargs)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Event(Base):
    """A class to represent a transient astrophysical Event.

    Events can be linked to Notices, with a specific event (e.g. GW170817)
    potentially producing multiple Notices as the skymap is updated.

    Parameters
    ----------
    name : string
        a unique, human-readable identifier for the event
    type : string
        the type of event, e.g. GW, GRB
    origin : string
        the group that produced the event, e.g. LVC, Fermi, GAIA

    time : string, `astropy.time.Time` or datetime.datetime, optional
        time the event occurred (or at least was first detected)

    When created the instance can be linked to the following other tables as parameters,
    otherwise they are populated when it is added to the database:

    Primary relationships
    ---------------------
    notices : list of `Notice`, optional
        the Notices relating to this Event, if any

    Attributes
    ----------
    db_id : int
        primary database key
        only populated when the instance is added to the database

    Secondary relationships
    -----------------------
    surveys : list of `gtecs.obs.database.Survey`
        the Surveys relating to this Event, if any

    """

    # Set corresponding SQL table name
    __tablename__ = 'events'
    __table_args__ = {'schema': 'alert'}

    # Primary key
    db_id = Column('id', Integer, primary_key=True)

    # Columns
    name = Column(String(255), nullable=False, unique=True, index=True)
    type = Column(String(255), nullable=False, index=True)  # noqa: A003
    origin = Column(String(255), nullable=False)
    time = Column(DateTime, nullable=True, default=None)

    # Foreign relationships
    notices = relationship(
        'Notice',
        order_by='Notice.db_id',
        back_populates='event',
    )

    # Secondary relationships
    surveys = relationship(
        'Survey',
        order_by='Survey.db_id',
        secondary='alert.notices',
        primaryjoin='Notice.event_id == Event.db_id',
        secondaryjoin='Survey.db_id == Notice.survey_id',
        backref=backref(  # NB Use legacy backref to add corresponding relationship to Surveys
            'event',
            uselist=False,
        ),
        viewonly=True,
    )

    def __repr__(self):
        strings = ['db_id={}'.format(self.db_id),
                   'name={}'.format(self.name),
                   'type={}'.format(self.type),
                   'origin={}'.format(self.origin),
                   'time={}'.format(self.time),
                   ]
        return 'Event({})'.format(', '.join(strings))

    @validates('time')
    def validate_times(self, key, field):
        """Use validators to allow various types of input for times."""
        if field is None:
            # time is nullable
            return None

        if isinstance(field, datetime.datetime):
            value = field.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(field, Time):
            field.precision = 0  # no D.P on seconds
            value = field.iso
        else:
            # just hope the string works!
            value = str(field)
        return value


class Notice(Base):
    """An alert Notice relating to an astrophysical Event.

    Parameters
    ----------
    ivorn : string
        The Notice IVORN (IVOA Resource Name).
    received : datetime.datetime
        The time the Notice was received.
    payload : bytes
        The VOEvent payload in JSON format, stored as binary data.

    skymap : `gototile.skymap.Skymap`, optional
        The skymap associated with this Notice, if any.
        The skymap is stored in the database as binary data.

    When created the instance can be linked to the following other tables as parameters,
    otherwise they are populated when it is added to the database:

    Primary relationships
    ---------------------
    event : `Event`, optional
        the Event this Notice is related to, if any
        can also be added with the event_id parameter
    survey : `gtecs.obs.database.Survey`, optional
        the Survey created from this Notice, if any
        can also be added with the survey_id parameter

    Attributes
    ----------
    db_id : int
        primary database key
        only populated when the instance is added to the database

    Secondary relationships
    -----------------------
    targets : list of `gtecs.obs.database.Target`
        the Targets created from this Notice, if any

    """

    # Set corresponding SQL table name
    __tablename__ = 'notices'
    __table_args__ = {'schema': 'alert'}

    # Primary key
    db_id = Column('id', Integer, primary_key=True)

    # Columns
    ivorn = Column(String(255), nullable=False, unique=True)
    received = Column(DateTime, nullable=False, index=True, server_default=func.now())
    payload = Column(LargeBinary, nullable=False)
    skymap = Column(LargeBinary, nullable=True)

    # Foreign keys
    event_id = Column(Integer, ForeignKey('alert.events.id'), nullable=True)
    survey_id = Column(Integer, ForeignKey('obs.surveys.id'), nullable=True)

    # Foreign relationships
    event = relationship(
        'Event',
        uselist=False,
        back_populates='notices',
    )
    survey = relationship(
        'Survey',
        uselist=False,
        backref=backref(  # NB Use legacy backref to add corresponding relationship to Surveys
            'notices',
            order_by='Notice.db_id',
        ),
    )

    # Secondary relationships
    targets = relationship(
        'Target',
        order_by='Target.db_id',
        secondary='obs.surveys',
        primaryjoin='Survey.db_id == Notice.survey_id',
        secondaryjoin='Survey.db_id == Target.survey_id',
        backref=backref(  # NB Use legacy backref to add corresponding relationship to Targets
            'notice',
            uselist=False,
        ),
        viewonly=True,
    )

    def __repr__(self):
        strings = ['ivorn={}'.format(self.ivorn),
                   'received={}'.format(self.received),
                   'event_id={}'.format(self.event_id),
                   'survey_id={}'.format(self.survey_id),
                   ]
        return 'Notice({})'.format(', '.join(strings))

    @validates('received')
    def validate_times(self, key, field):
        """Use validators to allow various types of input for times."""
        if field is None:
            # time is nullable
            return None

        if isinstance(field, datetime.datetime):
            value = field.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(field, Time):
            field.precision = 0  # no D.P on seconds
            value = field.iso
        else:
            # just hope the string works!
            value = str(field)
        return value

    @classmethod
    def from_gcn(cls, notice):
        """Create a database-linked Notice entry from a transient notice message."""
        if notice.skymap is None:
            notice.get_skymap()
        if notice.skymap is None:
            # Can't raise an error, it could be a retraction
            skymap_bytes = None
        else:
            if notice.skymap_file is None:
                # We created our own Skymap
                # So we have to save it to a file and read it back in, which is awkward..
                path = f'/tmp/skymap_{Time.now().isot}.fits'
                notice.skymap.save(path)
            else:
                # The skymap was downloaded to a temp file, so we need to check if it still exists
                if not os.path.exists(notice.skymap_file):
                    # We'll need to redownload it
                    notice.get_skymap()
                path = notice.skymap_file
            # Now open the file and read the bytes
            with open(path, 'rb') as f:
                skymap_bytes = f.read()

        db_notice = cls(
            ivorn=notice.ivorn,
            received=notice.creation_time,
            payload=notice.payload,
            skymap=skymap_bytes,
        )
        return db_notice

    @property
    def gcn(self):
        """Create a gtecs.alert.notices.Notice class (or subclass) from this Notice."""
        notice = EventNotice.from_payload(self.payload)
        if self.skymap is not None:
            notice.skymap = skymap_from_bytes(self.skymap)
        return notice
