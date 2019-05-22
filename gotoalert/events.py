"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus

from astroplan import FixedTarget

from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time

from gototile.skymap import SkyMap

import numpy as np

import voeventdb.remote.apiv1 as vdb

import voeventparse as vp

# Define interesting events we want to process
# Primary key is the GCN Packet_Type
# This should be in params
EVENT_DICTONARY = {150: {'notice_type': 'LVC_PRELIMINARY',
                         'event_type': 'GW',
                         'source': 'LVC',
                         },
                   151: {'notice_type': 'LVC_INITIAL',
                         'event_type': 'GW',
                         'source': 'LVC',
                         },
                   152: {'notice_type': 'LVC_UPDATE',
                         'event_type': 'GW',
                         'source': 'LVC',
                         },
                   112: {'notice_type': 'FERMI_GBM_GND_POS',
                         'event_type': 'GRB',
                         'source': 'Fermi',
                         },
                   115: {'notice_type': 'FERMI_GBM_FIN_POS',
                         'event_type': 'GRB',
                         'source': 'Fermi',
                         },
                   61: {'notice': 'SWIFT_BAT_GRB_POS',
                        'event_type': 'GRB',
                        'source': 'Swift',
                        },
                   }


class Event(object):
    """A class to represent a single VOEvent.

    Some Events are better represented as one of the more specalised subclasses.

    Use Event.from_payload() or Event.from_ivorn() to create an appropriate event.
    """

    def __init__(self, payload):
        # Store the creation time
        self.creation_time = Time.now()

        # Store the payload
        self.payload = payload

        # Load the payload using voeventparse
        self.voevent = vp.loads(self.payload)
        self.top_params = vp.get_toplevel_params(self.voevent)
        self.group_params = vp.get_grouped_params(self.voevent)

        # Get key attributes:
        # IVORN
        self.ivorn = self.voevent.attrib['ivorn']
        # Using the official IVOA terms (ivo://authorityID/resourceKey#local_ID):
        self.authorityID = self.ivorn.split('/')[2]
        self.resourceKey = self.ivorn.split('/')[3].split('#')[0]
        self.local_ID = self.ivorn.split('/')[3].split('#')[1]
        # Using some easier terms to understand:
        self.authority = self.authorityID
        self.publisher = self.resourceKey
        self.title = self.local_ID

        # Role (observation/test/...)
        self.role = self.voevent.attrib['role']

        # Event time
        event_time = vp.convenience.get_event_time_as_utc(self.voevent, index=0)
        if event_time:
            self.time = Time(event_time)
        else:
            # Some test events don't have times
            self.time = None

        # Contact email
        try:
            self.contact = self.voevent.Who.Author.contactEmail
        except AttributeError:
            self.contact = None

        # GCN packet type
        self.packet_type = self._get_packet_type(payload)

        # Set default attirbutes
        # The subclasses for "interesting" events will overwrite these
        self.interesting = False
        self.notice = 'Unknown'
        self.type = 'Unknown'
        self.source = 'Unknown'

    def __repr__(self):
        return '{}(ivorn={})'.format(self.__class__.__name__, self.ivorn)

    @staticmethod
    def _get_packet_type(payload):
        """Get the packet type from a VOEvent payload."""
        voevent = vp.loads(payload)
        top_params = vp.get_toplevel_params(voevent)
        try:
            packet_type = int(top_params['Packet_Type']['value'])
        except KeyError:
            # If it's a VOEvent but not a GCN it won't have a packet type (e.g. Gaia alerts)
            packet_type = None
        return packet_type

    @classmethod
    def from_payload(cls, payload):
        """Create an Event from a VOEvent payload."""
        # Chose a more detailed subclass based on GCN Packet Type, if there is one
        packet_type = cls._get_packet_type(payload)
        if not packet_type or packet_type not in EVENT_DICTONARY:
            # Not a GCN, or not a recognised packet type
            event_class = Event
        else:
            event_type = EVENT_DICTONARY[packet_type]['event_type']
            if event_type == 'GW':
                event_class = GWEvent
            elif event_type == 'GRB':
                event_class = GRBEvent
            else:
                # This shouldn't happen?
                event_class = Event

        # Create and return the instance
        return event_class(payload)

    @classmethod
    def from_ivorn(cls, ivorn):
        """Create an Event by querying the 4pisky VOEvent database."""
        payload = vdb.packet_xml(ivorn)
        return cls.from_payload(payload)

    def archive(self, path):
        """Archive this event in the config directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        savepath = os.path.join(path, filename)
        with open(savepath, 'wb') as f:
            f.write(self.payload)


class GWEvent(Event):
    """A class to represent a Gravitational Wave Event."""

    def __init__(self, payload):
        super().__init__(payload)

        # Default params
        self.interesting = True
        self.notice = EVENT_DICTONARY[self.packet_type]['notice_type']
        self.type = 'GW'
        self.source = EVENT_DICTONARY[self.packet_type]['source']

        # Get the event ID (e.g. S190510g)
        self.id = self.top_params['GraceID']['value']

        # Create our own event name (e.g. LVC_S190510g)
        self.name = '{}_{}'.format(self.source, self.id)

        # Get skymap url
        for group in self.group_params:
            if 'skymap_fits' in self.group_params[group]:
                self.skymap_url = self.group_params[group]['skymap_fits']['value']
                self.skymap_type = group

        # Download the skymap
        self.skymap = SkyMap.from_fits(self.skymap_url)
        # Store basic info
        self.skymap.object = self.name
        self.skymap.objid = self.id
        # Don't regrade here, let the user do that if they want to


class GRBEvent(Event):
    """A class to represent a Gamma-Ray Burst Event."""

    def __init__(self, payload):
        super().__init__(payload)

        # Default params
        self.interesting = True
        self.notice = EVENT_DICTONARY[self.packet_type]['notice_type']
        self.type = 'GRB'
        self.source = EVENT_DICTONARY[self.packet_type]['source']

        # Get the event ID (e.g. 579943502)
        self.id = self.top_params['TrigID']['value']

        # Create our own event name (e.g. Fermi_579943502)
        self.name = '{}_{}'.format(self.source, self.id)

        # Position coordinates
        self.position = vp.get_event_position(self.voevent)
        self.coord = SkyCoord(ra=self.position.ra, dec=self.position.dec, unit=self.position.units)
        self.target = FixedTarget(self.coord)

        # Position error
        self.coord_error = Angle(self.position.err, unit=self.position.units)
        if self.source == 'Fermi':
            self.systematic_error = Angle(3.71, unit='deg')
        else:
            self.systematic_error = Angle(0, unit='deg')
        self.total_error = Angle(np.sqrt(self.coord_error ** 2 + self.systematic_error ** 2),
                                 unit='deg')

        # Galactic coordinates
        self.gal_lat = self.coord.galactic.b.value
        galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
        self.gal_dist = self.coord.galactic.separation(galactic_center).value

        # Create a Gaussian skymap
        self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                           self.coord.dec.deg,
                                           self.total_error.deg,
                                           nside=128)
        # Store basic info
        self.skymap.object = self.name
        self.skymap.objid = self.id
