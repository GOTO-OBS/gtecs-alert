"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus
from urllib.request import urlopen

from astroplan import FixedTarget

from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time
from astropy.utils.data import download_file

from gototile.skymap import SkyMap

import numpy as np

import voeventdb.remote.apiv1 as vdb

import voeventparse as vp

from . import params
from .strategy import get_event_strategy


# Define interesting events we want to process
# Primary key is the GCN Packet_Type
# This should be in params
EVENT_DICTIONARY = {163: {'notice_type': 'LVC_EARLY_WARNING',
                          'event_type': 'GW',
                          'source': 'LVC',
                          },
                    150: {'notice_type': 'LVC_PRELIMINARY',
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
                    164: {'notice_type': 'LVC_RETRACTION',
                          'event_type': 'GW_RETRACTION',
                          'source': 'LVC',
                          },
                    115: {'notice_type': 'FERMI_GBM_FIN_POS',
                          'event_type': 'GRB',
                          'source': 'Fermi',
                          },
                    61: {'notice_type': 'SWIFT_BAT_GRB_POS',
                         'event_type': 'GRB',
                         'source': 'Swift',
                         },
                    173: {'notice_type': 'ICECUBE_ASTROTRACK_GOLD',
                          'event_type': 'NU',
                          'source': 'IceCube',
                          },
                    174: {'notice_type': 'ICECUBE_ASTROTRACK_BRONZE',
                          'event_type': 'NU',
                          'source': 'IceCube',
                          },
                    176: {'notice_type': 'ICECUBE_CASCADE',
                          'event_type': 'NU',
                          'source': 'IceCube',
                          },
                    }


class Event(object):
    """A class to represent a single VOEvent.

    Some Events are better represented as one of the more specialised subclasses.

    Use one of the following classmethods to to create an appropriate event:
        - Event.from_file()
        - Event.from_url()
        - Event.from_ivorn()
        - Event.from_payload()
    """

    def __init__(self, payload):
        # Store the creation time
        self.creation_time = Time.now()

        # Store the payload
        self.payload = payload

        # Load the payload using voeventparse
        self.voevent = vp.loads(self.payload)

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

        # Set default attributes
        # The subclasses for "interesting" events will overwrite these
        self.notice = 'Unknown'
        self.type = 'Unknown'
        self.source = 'Unknown'
        self.position = None
        self.coord = None
        self.target = None
        self.skymap = None
        self.properties = {}
        self.strategy = None

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
        if not packet_type or packet_type not in EVENT_DICTIONARY:
            # Not a GCN, or not a recognised packet type
            event_class = Event
        else:
            event_type = EVENT_DICTIONARY[packet_type]['event_type']
            if event_type == 'GW':
                event_class = GWEvent
            elif event_type == 'GW_RETRACTION':
                event_class = GWRetractionEvent
            elif event_type == 'GRB':
                event_class = GRBEvent
            elif event_type == 'NU':
                event_class = NUEvent
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

    @classmethod
    def from_url(cls, url):
        """Create an Event by downloading the VOEvent from the given URL."""
        with urlopen(url) as r:
            payload = r.read()
        return cls.from_payload(payload)

    @classmethod
    def from_file(cls, filepath):
        """Create an Event by reading a VOEvent XML file."""
        with open(filepath, 'rb') as f:
            payload = f.read()
        return cls.from_payload(payload)

    @property
    def interesting(self):
        """Check if this Event is classified as interesting."""
        if self.type == 'Unknown':
            return False
        if self.role in params.IGNORE_ROLES:
            return False
        return True

    def archive(self, path):
        """Archive this event in the given directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        savepath = os.path.join(path, filename)
        with open(savepath, 'wb') as f:
            f.write(self.payload)
        return savepath

    def get_strategy(self):
        """Get the event observing strategy."""
        self.strategy = get_event_strategy(self)
        return self.strategy


class GWEvent(Event):
    """A class to represent a Gravitational Wave Event."""

    def __init__(self, payload):
        super().__init__(payload)

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Default params
        self.notice = EVENT_DICTIONARY[self.packet_type]['notice_type']
        self.type = 'GW'
        self.source = EVENT_DICTIONARY[self.packet_type]['source']

        # Get the event ID (e.g. S190510g)
        self.id = top_params['GraceID']['value']

        # Create our own event name (e.g. LVC_S190510g)
        self.name = '{}_{}'.format(self.source, self.id)

        # Get info from the VOEvent
        # See https://emfollow.docs.ligo.org/userguide/content.html#notice-contents
        self.far = float(top_params['FAR']['value'])
        self.gracedb_url = top_params['EventPage']['value']
        self.instruments = top_params['Instruments']['value']
        self.group = top_params['Group']['value']  # CBC or Burst
        self.pipeline = top_params['Pipeline']['value']

        # Get classification probabilities and properties
        if self.group == 'CBC':
            classification_dict = group_params.allitems()[1][1]  # Horrible, but blame XML
            self.classification = {key: float(classification_dict[key]['value'])
                                   for key in classification_dict}
            properties_dict = group_params.allitems()[2][1]
            self.properties = {key: float(properties_dict[key]['value'])
                               for key in properties_dict}
        else:
            self.classification = {}
            self.properties = {}

        # Get skymap URL
        for group in group_params:
            if 'skymap_fits' in group_params[group]:
                self.skymap_url = group_params[group]['skymap_fits']['value']
                self.skymap_type = group

        # Don't download the skymap here, it may well be very large.
        # Only do it when it's absolutely necessary
        # These params will only be set once the skymap is downloaded
        self.distance = np.inf
        self.distance_error = 0
        self.contour_areas = {0.5: None, 0.9: None}

    def get_skymap(self, nside=128):
        """Download the Event skymap and return it as a `gototile.skymap.SkyMap object."""
        if self.skymap:
            return self.skymap

        # Download the skymap from the URL
        # The file gets stored in /tmp/
        # Don't cache, force redownload every time
        # https://github.com/GOTO-OBS/goto-alert/issues/36
        self.skymap_file = download_file(self.skymap_url, cache=False)

        # Create the skymap object and regrade
        self.skymap = SkyMap.from_fits(self.skymap_file)
        self.skymap.regrade(nside)

        # Store basic info on the skymap
        self.skymap.object = self.name
        self.skymap.objid = self.id

        # Get info from the skymap header
        try:
            self.distance = self.skymap.header['distmean']
            self.distance_error = self.skymap.header['diststd']
        except KeyError:
            # Older skymaps (& Burst?) might not have distances
            self.distance = np.inf
            self.distance_error = 0

        # Get info from the skymap itself
        self.contour_areas = {}
        for contour in [0.5, 0.9]:
            self.contour_areas[contour] = self.skymap.get_contour_area(contour)

        return self.skymap


class GWRetractionEvent(Event):
    """A class to represent a Gravitational Wave Retraction alert."""

    def __init__(self, payload):
        super().__init__(payload)

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)

        # Default params
        self.notice = EVENT_DICTIONARY[self.packet_type]['notice_type']
        self.type = 'GW_RETRACTION'
        self.source = EVENT_DICTIONARY[self.packet_type]['source']

        # Get the event ID (e.g. S190510g)
        self.id = top_params['GraceID']['value']

        # Create our own event name (e.g. LVC_S190510g)
        self.name = '{}_{}'.format(self.source, self.id)

        # Get info from the VOEvent
        # Retractions have far fewer params
        self.gracedb_url = top_params['EventPage']['value']


class GRBEvent(Event):
    """A class to represent a Gamma-Ray Burst Event."""

    def __init__(self, payload):
        super().__init__(payload)

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Default params
        self.notice = EVENT_DICTIONARY[self.packet_type]['notice_type']
        self.type = 'GRB'
        self.source = EVENT_DICTIONARY[self.packet_type]['source']

        # Get the event ID (e.g. 579943502)
        self.id = top_params['TrigID']['value']

        # Create our own event name (e.g. Fermi_579943502)
        self.name = '{}_{}'.format(self.source, self.id)

        # Get properties from the VOEvent
        if self.source == 'Fermi':
            self.properties = {key: group_params['Trigger_ID'][key]['value']
                               for key in group_params['Trigger_ID']
                               if key != 'Long_short'}
            try:
                self.duration = group_params['Trigger_ID']['Long_short']['value']
            except KeyError:
                # Some don't have the duration
                self.duration = 'unknown'
        elif self.source == 'Swift':
            self.properties = {key: group_params['Solution_Status'][key]['value']
                               for key in group_params['Solution_Status']}
        for key in self.properties:
            if self.properties[key] == 'true':
                self.properties[key] = True
            elif self.properties[key] == 'false':
                self.properties[key] = False

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

        # Try creating the Fermi skymap url
        # Fermi haven't actually updated their alerts to include the URL to the HEALPix skymap,
        # but we can try and create it based on the typical location.
        try:
            old_url = top_params['LightCurve_URL']['value']
            skymap_url = old_url.replace('lc_medres34', 'healpix_all').replace('.gif', '.fit')
            self.skymap_url = skymap_url
        except Exception:
            # Worth a try, fall back to creating our own
            self.skymap_url = None

        # Don't create or download the skymap here, it may well be very large.
        # Only do it when it's absolutely necessary
        # These params will only be set once the skymap is downloaded
        self.contour_areas = {0.5: None, 0.9: None}

    def get_skymap(self, nside=128):
        """Download the Event skymap and return it as a `gototile.skymap.SkyMap object."""
        if self.skymap:
            return self.skymap

        # Try downloading the Fermi skymap
        if self.skymap_url:
            try:
                # Download the skymap from the URL, create the skymap object and regrade
                self.skymap_file = download_file(self.skymap_url, cache=False)
                self.skymap = SkyMap.from_fits(self.skymap_file)
                self.skymap.regrade(nside)
            except Exception:
                # Worth a try, fall back to creating our own
                pass

        # Create a Gaussian skymap (if we didn't download one above)
        if not self.skymap:
            self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                               self.coord.dec.deg,
                                               self.total_error.deg,
                                               nside=nside)

        # Store basic info on the skymap
        self.skymap.object = self.name
        self.skymap.objid = self.id

        # Get info from the skymap itself
        self.contour_areas = {}
        for contour in [0.5, 0.9]:
            self.contour_areas[contour] = self.skymap.get_contour_area(contour)

        return self.skymap


class NUEvent(Event):
    """A class to represent a Neutrino (NU) Event."""

    def __init__(self, payload):
        super().__init__(payload)

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        # group_params = vp.get_grouped_params(self.voevent)

        # Default params
        self.notice = EVENT_DICTIONARY[self.packet_type]['notice_type']
        self.type = EVENT_DICTIONARY[self.packet_type]['event_type']
        self.source = EVENT_DICTIONARY[self.packet_type]['source']

        # Get the run and event ID (e.g. 13311922683750)
        self.id = top_params['AMON_ID']['value']

        # Create our own event name (e.g. ICECUBE_13311922683750)
        self.name = '{}_{}'.format(self.source, self.id)

        # Get info from the VOEvent
        # signalness: the probability this is an astrophysical signal relative to backgrounds
        self.signalness = float(top_params['signalness']['value'])
        self.far = float(top_params['FAR']['value'])

        # Position coordinates
        self.position = vp.get_event_position(self.voevent)
        self.coord = SkyCoord(ra=self.position.ra, dec=self.position.dec, unit=self.position.units)
        self.target = FixedTarget(self.coord)

        # Position error
        self.coord_error = Angle(self.position.err, unit=self.position.units)

        # Systematic error for cascade event is given, so = 0
        if self.notice == 'ICECUBE_CASCADE':
            self.systematic_error = Angle(0, unit='deg')
        else:
            self.systematic_error = Angle(.2, unit='deg')
        self.total_error = Angle(np.sqrt(self.coord_error ** 2 + self.systematic_error ** 2),
                                 unit='deg')

        # Enclosed skymap url for CASCADE_EVENT, but others
        # Get skymap URL
        if 'skymap_fits' in top_params:
            self.skymap_url = top_params['skymap_fits']['value']
        else:
            self.skymap_url = None

        # Don't download the skymap here, it may well be very large.
        # Only do it when it's absolutely necessary
        # These params will only be set once the skymap is downloaded
        self.contour_areas = {0.5: None, 0.9: None}

    def get_skymap(self, nside=128):
        """Download the Event skymap and return it as a `gototile.skymap.SkyMap object."""
        if self.skymap:
            return self.skymap

        # Download the skymap from the URL
        # The file gets stored in /tmp/
        # Don't cache, force redownload every time
        # https://github.com/GOTO-OBS/goto-alert/issues/36
        if self.skymap_url:
            try:
                self.skymap_file = download_file(self.skymap_url, cache=False)
                self.skymap = SkyMap.from_fits(self.skymap_file)
                self.skymap.regrade(nside)
            except Exception:
                # Fall back to creating our own
                pass

        # Create a Gaussian skymap (if we didn't download one above)
        if not self.skymap:
            self.skymap = SkyMap.from_position(self.coord.ra.deg,
                                               self.coord.dec.deg,
                                               self.total_error.deg,
                                               nside=nside)

        # Store basic info on the skymap
        self.skymap.object = self.name
        self.skymap.objid = self.id

        # Get info from the skymap itself
        self.contour_areas = {}
        for contour in [0.5, 0.9]:
            self.contour_areas[contour] = self.skymap.get_contour_area(contour)

        return self.skymap
