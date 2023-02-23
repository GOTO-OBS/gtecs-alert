"""Alert database archive functions and ORM."""

import datetime
import os
from contextlib import contextmanager
from gzip import GzipFile
from io import BytesIO

from astropy.io import fits
from astropy.time import Time

from gototile.skymap import SkyMap

from gtecs.common.database import get_session
from gtecs.obs.database.models import Base

from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy import func
from sqlalchemy.orm import backref, relationship, validates

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

    When created the instance can be linked to the following other tables as parameters,
    otherwise they are populated when it is added to the database:

    Primary relationships
    ---------------------
    survey : `gtecs.obs.database.Survey`, optional
        the Survey created from this VOEvent, if any
        can also be added with the survey_id parameter

    Attributes
    ----------
    db_id : int
        primary database key
        only populated when the instance is added to the database

    Secondary relationships
    -----------------------
    event : `gtecs.obs.database.Event`
        the Event this VOEvent is part of, if any

    """

    # Set corresponding SQL table name
    __tablename__ = 'voevents'
    __table_args__ = {'schema': 'alert'}

    # Primary key
    db_id = Column('id', Integer, primary_key=True)

    # Columns
    ivorn = Column(String(255), nullable=False, unique=True)
    received = Column(DateTime, nullable=False, index=True, server_default=func.now())
    payload = Column(LargeBinary, nullable=False)
    skymap = Column(LargeBinary, nullable=True)

    # Foreign keys
    survey_id = Column(Integer, ForeignKey('obs.surveys.id'), nullable=True)

    # Foreign relationships
    survey = relationship(
        'Survey',
        uselist=False,
        backref=backref(  # NB Use legacy backref to add corresponding relationship to Surveys
            'voevent',
            uselist=False,
        ),
    )

    # Secondary relationships
    event = relationship(
        'Event',
        uselist=False,
        secondary='obs.surveys',
        primaryjoin='Survey.db_id == VOEvent.survey_id',
        secondaryjoin='Survey.event_id == Event.db_id',
        backref=backref(  # NB Use legacy backref to add corresponding relationship to Events
            'voevents',
            order_by='VOEvent.db_id',
        ),
        viewonly=True,
    )

    def __repr__(self):
        strings = ['ivorn={}'.format(self.ivorn),
                   'received={}'.format(self.received),
                   'survey_id={}'.format(self.survey_id),
                   ]
        return 'VOEvent({})'.format(', '.join(strings))

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
    def from_event(cls, event):
        """Create a VOEvent from an Event class."""
        if event.skymap is None:
            event.get_skymap()
        if event.skymap is None:
            # Can't raise an error, it could be a retraction
            skymap_bytes = None
        else:
            if event.skymap_file is None:
                # We created our own Skymap
                # So we have to save it to a file and read it back in, which is awkward..
                path = f'/tmp/skymap_{Time.now().isot}.fits'
                event.skymap.save(path)
            else:
                # The skymap was downloaded to a temp file, so we need to check if it still exists
                if not os.path.exists(event.skymap_file):
                    # We'll need to redownload it
                    event.get_skymap()
                path = event.skymap_file
            # Now open the file and read the bytes
            with open(path, 'rb') as f:
                skymap_bytes = f.read()

        voevent = cls(
            ivorn=event.ivorn,
            received=event.creation_time,
            payload=event.payload,
            skymap=skymap_bytes,
        )
        return voevent

    @property
    def event(self):
        """Get the Event class from the VOEvent payload."""
        event = Event.from_payload(self.payload)
        if self.skymap is not None:
            try:
                hdu = fits.open(BytesIO(self.skymap))
            except OSError:
                # It might be compressed
                gzip = GzipFile(fileobj=BytesIO(self.skymap), mode='rb')
                hdu = fits.open(gzip)
            event.skymap = SkyMap.from_fits(hdu)
        return event
