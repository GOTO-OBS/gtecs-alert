"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus

from astroplan import FixedTarget

from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time

from gototile.skymap import SkyMap

import voeventparse as vp


ALERT_DICTIONARY = {'XRT_Pos': {'type': 'GRB',
                                'source': 'SWIFT',
                                'base_name': 'Swift_XRT_POS'},
                    'BAT_GRB_Pos': {'type': 'GRB',
                                    'source': 'SWIFT',
                                    'base_name': 'Swift_BAT_GRB_POS'},
                    'GBM_Gnd_Pos': {'type': 'GRB',
                                    'source': 'Fermi',
                                    'base_name': 'Fermi_GMB_GND_POS'},
                    'gwnet': {'type': 'GW',
                              'source': 'GCN_SENDER',
                              'base_name': 'LVC_GW'},
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
            self.coord_error = Angle(position.err, unit=position.units)
            self.target = FixedTarget(self.coord)

            # (Galactic)
            self.gal_lat = self.coord.galactic.b.value
            galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
            self.gal_dist = self.coord.galactic.separation(galactic_center).value
        except AttributeError:
            # Probably a LVC skympap
            self.coord = None
            self.coord_error = None
            self.target = None
            self.gal_la = None
            self.gal_dist = None

        # Get skymap url, if there is one
        group_params = vp.get_grouped_params(self.voevent)
        try:
            self.skymap_url = group_params['bayestar']['skymap_fits']['value']
        except KeyError:
            self.skymap_url = None
        self.skymap = None

        if not any([key in self.ivorn for key in ALERT_DICTIONARY]):
            # The event doesn't match any ones we care about
            self.interesting = False
            self.type = 'Unknown'
            self.source = self.ivorn.split('/')[-1].split('#')[0]
            self.base_name = 'Unknown'

        else:
            # The IVORN must match one of the events we're looking for
            self.interesting = True
            for key in ALERT_DICTIONARY:
                if key in self.ivorn:
                    self.type = ALERT_DICTIONARY[key]['type']
                    self.source = ALERT_DICTIONARY[key]['source']
                    self.base_name = ALERT_DICTIONARY[key]['base_name']

            # Sanity check that the sources match
            ivorn_source = self.ivorn.split('/')[-1].split('#')[0]
            if self.source.upper() != ivorn_source.upper():
                raise ValueError('Mismatched sources: {} and {}'.format(self.source.upper(),
                                                                        ivorn_source.upper()))

        # Get the trigger ID, if there is one
        top_params = vp.get_toplevel_params(self.voevent)
        if self.type == 'GRB':
            self.trigger_id = int(top_params['TrigID']['value'])
            self.name = '{}_{:.0f}'.format(self.base_name, self.trigger_id)
        elif self.type == 'GW':
            self.trigger_id = 0
            self.name = top_params['GraceID']['value']
        else:
            self.trigger_id = 0
            self.name = '{}_{:.0f}'.format(self.base_name, self.trigger_id)

        # Get contact email, if there is one
        try:
            self.contact = self.voevent.Who.Author.contactEmail
        except AttributeError:
            self.contact = None

    def __repr__(self):
            return 'Event(name={}, ivorn={})'.format(self.name, self.ivorn)

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
        else:
            self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                               self.coord.dec.deg,
                                               self.coord_error.deg,
                                               nside)
        return self.skymap
