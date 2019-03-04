"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus

from astroplan import FixedTarget

from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time

from gototile.skymap import SkyMap

import numpy as np

import voeventparse as vp

# Define interesting events we want to process
# Primary key is the VOEvent Packet_Type
EVENT_DICTONARY = {  # Swift GRBs
                     61: {'notice': 'SWIFT_BAT_GRB_POS',
                          'type': 'GRB',
                          'source': 'Swift',
                          'systematic_error': 0,
                          },
                     # 67: {'notice': 'SWIFT_XRT_POS',
                     #      'type': 'GRB',
                     #      'source': 'Swift',
                     #      'systematic_error': 0,
                     #      },
                     # Fermi GRBs
                     112: {'notice': 'FERMI_GBM_GND_POS',
                           'type': 'GRB',
                           'source': 'Fermi',
                           'systematic_error': 3.71,
                           },
                     115: {'notice': 'FERMI_GBM_FIN_POS',
                           'type': 'GRB',
                           'source': 'Fermi',
                           'systematic_error': 3.71,  # Might be different
                           },
                     # LVC GW events
                     150: {'notice': 'LVC_PRELIMINARY',
                           'type': 'GW',
                           'source': 'LVC',
                           'systematic_error': 0,
                           },
                     151: {'notice': 'LVC_INITIAL',
                           'type': 'GW',
                           'source': 'LVC',
                           'systematic_error': 0,
                           },
                     152: {'notice': 'LVC_UPDATE',
                           'type': 'GW',
                           'source': 'LVC',
                           'systematic_error': 0,
                           },
}


class Event(object):
    """A simple class to represent a single VOEvent."""

    def __init__(self, payload):
        self.creation_time = Time.now()
        self.payload = payload

        # Load the payload using voeventparse
        self.voevent = vp.loads(self.payload)

        # Get key attributes
        self.ivorn = self.voevent.attrib['ivorn']
        self.role = self.voevent.attrib['role']

        # Get packet type
        try:
            top_params = vp.get_toplevel_params(self.voevent)
            self.packet_type = int(top_params['Packet_Type']['value'])
        except Exception:
            # Some test events don't have packet types
            return

        if self.packet_type not in EVENT_DICTONARY:
            # The event doesn't match any ones we care about
            self.interesting = False
            self.notice = 'Unknown'
            self.type = 'Unknown'
            self.source = 'Unknown'
            self.systematic_error = Angle(0, unit='deg')
        else:
            # The packet type must match one of the events we're looking for
            self.interesting = True
            self.notice = EVENT_DICTONARY[self.packet_type]['notice']
            self.type = EVENT_DICTONARY[self.packet_type]['type']
            self.source = EVENT_DICTONARY[self.packet_type]['source']
            self.systematic_error = Angle(EVENT_DICTONARY[self.packet_type]['systematic_error'],
                                          unit='deg')

        # Get event time
        event_time = vp.convenience.get_event_time_as_utc(self.voevent, index=0)
        if event_time is None:
            # Sometimes we might get system test events from the server.
            # They (annoyingly) don't actually have an event attached, even a fake one,
            # so don't have a time. Just return here, the handler will ignore it.
            self.time = None
            return
        self.time = Time(event_time)

        # Get event position
        try:
            # (RA/DEC)
            position = vp.get_event_position(self.voevent)
            self.coord = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
            self.target = FixedTarget(self.coord)

            # Error
            self.coord_error = Angle(position.err, unit=position.units)
            self.total_error = Angle(np.sqrt(self.coord_error ** 2 + self.systematic_error ** 2),
                                     unit='deg')

            # (Galactic)
            self.gal_lat = self.coord.galactic.b.value
            galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
            self.gal_dist = self.coord.galactic.separation(galactic_center).value
        except AttributeError:
            # Probably a LVC skympap
            self.coord = None
            self.coord_error = None
            self.target = None
            self.gal_lat = None
            self.gal_dist = None

        # Get skymap url, if there is one
        group_params = vp.get_grouped_params(self.voevent)
        try:
            self.skymap_url = group_params['bayestar']['skymap_fits']['value']
        except KeyError:
            self.skymap_url = None
        self.skymap = None

        # Get the trigger ID, if there is one
        if self.type == 'GRB':
            self.id = top_params['TrigID']['value']
        elif self.type == 'GW':
            self.id = top_params['GraceID']['value']
        else:
            self.id = '0'
        self.name = '{}_{}'.format(self.source, self.id)

        # Get contact email, if there is one
        try:
            self.contact = self.voevent.Who.Author.contactEmail
        except AttributeError:
            self.contact = None

    def __repr__(self):
            return 'Event(name={}, notice={}, type={})'.format(self.name, self.notice, self.type)

    def archive(self, path, log=None):
        """Archive this event in the config directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        with open(path + filename, 'wb') as f:
            f.write(self.payload)

        if log:
            log.info('Archived to {}'.format(path))

    def get_skymap(self, nside=128):
        """Create a GOTO-tile SkyMap for the event.

        If the Event is from the LVC then it should have a skymap url,
        if not then a Gaussian skymap is created based on the position error.

        """
        if self.skymap:
            return self.skymap

        if self.skymap_url:
            # HealPIX can download from a URL
            self.skymap = SkyMap.from_fits(self.skymap_url)
            self.skymap.regrade(nside)
        else:
            self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                               self.coord.dec.deg,
                                               self.total_error.deg,
                                               nside)

        # Add some basic info
        self.skymap.object = self.name
        self.skymap.objid = self.id

        return self.skymap
