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
        # Store the creation time
        self.creation_time = Time.now()

        # Store the payload
        self.payload = payload

        # Load the payload using voeventparse
        self.voevent = vp.loads(self.payload)
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

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
        self.name = self.local_ID

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
        try:
            self.packet_type = int(top_params['Packet_Type']['value'])
        except KeyError:
            # If it's not a GCN it won't have a Packet_Type (e.g. Gaia alerts)
            self.packet_type = None
            return

        # If the packet type isn't in the event dictionary (or it doesn't have one)
        # then it's not an interesting event to us.
        # Set some defaults and return here.
        if not self.packet_type or (self.packet_type not in EVENT_DICTONARY):
            self.interesting = False
            self.notice = 'Unknown'
            self.type = 'Unknown'
            self.source = 'Unknown'
            self.systematic_error = Angle(0, unit='deg')
            return

        # If we're still here then the packet type must match one of the events we're looking for
        self.interesting = True
        self.notice = EVENT_DICTONARY[self.packet_type]['notice']
        self.type = EVENT_DICTONARY[self.packet_type]['type']
        self.source = EVENT_DICTONARY[self.packet_type]['source']
        self.systematic_error = Angle(EVENT_DICTONARY[self.packet_type]['systematic_error'],
                                      unit='deg')

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
        self.skymap_url = None
        self.skymap_type = None
        for group in group_params:
            if 'skymap_fits' in group_params[group]:
                self.skymap_url = group_params[group]['skymap_fits']['value']
                self.skymap_type = group
        self.skymap = None

        # Get the trigger ID, if there is one
        if self.type == 'GRB':
            self.id = top_params['TrigID']['value']
        elif self.type == 'GW':
            self.id = top_params['GraceID']['value']
        else:
            self.id = '0'
        self.name = '{}_{}'.format(self.source, self.id)

    def __repr__(self):
        return 'Event(ivorn={})'.format(self.ivorn)

    @classmethod
    def from_ivorn(cls, ivorn):
        """Create an Event by querying the 4pisky VOEvent database."""
        payload = vdb.packet_xml(ivorn)
        return cls(payload)

    def archive(self, path):
        """Archive this event in the config directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        savepath = os.path.join(path, filename)
        with open(savepath, 'wb') as f:
            f.write(self.payload)

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
        elif self.coord:
            # Make a Gaussian one
            self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                               self.coord.dec.deg,
                                               self.total_error.deg,
                                               nside)
        else:
            raise ValueError('No skymap_url or central coordinate')

        # Add some basic info
        self.skymap.object = self.name
        self.skymap.objid = self.id

        return self.skymap
