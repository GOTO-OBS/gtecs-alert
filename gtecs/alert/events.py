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
from .strategy import get_strategy_details


class Event:
    """A class to represent a single VOEvent.

    Some Events are better represented as one of the more specialised subclasses.

    Use one of the following classmethods to to create an appropriate event:
        - Event.from_file()
        - Event.from_url()
        - Event.from_ivorn()
        - Event.from_payload()
    """

    def __init__(self, payload):
        self.creation_time = Time.now()

        # Load the payload using voeventparse
        self.payload = payload
        self.voevent = vp.loads(payload)

        # Store and format IVORN
        self.ivorn = self.voevent.attrib['ivorn']
        # Using the official IVOA terms (ivo://authorityID/resourceKey#local_ID):
        self.authorityID = self.ivorn.split('/')[2]
        self.resourceKey = self.ivorn.split('/')[3].split('#')[0]
        self.local_ID = self.ivorn.split('/')[3].split('#')[1]
        # Using some easier terms to understand:
        self.authority = self.authorityID
        self.publisher = self.resourceKey
        self.title = self.local_ID

        # Key attributes
        self.packet_type = self._get_packet_type(payload)
        self.role = self.voevent.attrib['role']
        try:
            self.time = Time(vp.convenience.get_event_time_as_utc(self.voevent, index=0))
        except Exception:
            # Some test events don't have times
            self.time = None
        try:
            self.contact = self.voevent.Who.Author.contactEmail
        except AttributeError:
            self.contact = None

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

    @staticmethod
    def _get_class(payload):
        """Get the correct class of Event by trying each subclass."""
        subclasses = [GWEvent, GWRetractionEvent, GRBEvent, NUEvent]
        for subclass in subclasses:
            try:
                return subclass(payload)
            except ValueError:
                pass
        return Event(payload)

    @classmethod
    def from_payload(cls, payload):
        """Create an Event from a VOEvent payload."""
        event = cls._get_class(payload)
        if cls != Event and cls != event.__class__:
            raise ValueError('Event subtype mismatch (`{}` detected)'.format(
                             event.__class__.__name__
                             ))
        return event

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
        out_path = os.path.join(path, filename)
        with open(out_path, 'wb') as f:
            f.write(self.payload)
        return out_path

    def get_skymap(self):
        """Return None."""
        return

    def get_strategy(self):
        """Return default strategy."""
        self.strategy = get_strategy_details()
        return self.strategy


class GWEvent(Event):
    """A class to represent a Gravitational Wave Event."""

    VALID_PACKET_TYPES = {
        163: 'LVC_EARLY_WARNING',
        150: 'LVC_PRELIMINARY',
        151: 'LVC_INITIAL',
        152: 'LVC_UPDATE',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_type not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_type} not valid for this event class')
        self.notice = self.VALID_PACKET_TYPES[self.packet_type]

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Basic attributes
        self.type = 'GW'
        self.source = 'LVC'
        self.id = top_params['GraceID']['value']  # e.g. S190510g
        self.name = '{}_{}'.format(self.source, self.id)  # e.g. LVC_S190510g

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

    def get_strategy(self):
        """Get the event observing strategy."""
        if self.strategy:
            return self.strategy

        # Decide which strategy to use
        if self.group == 'CBC':
            if self.properties['HasNS'] > 0.25:
                if self.distance < 400:
                    strategy = 'GW_CLOSE_NS'
                else:
                    # If the skymap hasn't been downloaded yet then we won't know the distance.
                    # It gets set to infinity, so it will default to the FAR strategy.
                    # TODO: we should raise a warning here?
                    strategy = 'GW_FAR_NS'
            else:
                if self.distance < 100:
                    strategy = 'GW_CLOSE_BH'
                else:
                    # TODO: Same here as above
                    strategy = 'GW_FAR_BH'
        elif self.group == 'Burst':
            strategy = 'GW_BURST'
        else:
            raise ValueError(f'Cannot determine observing strategy for group "{self.group}"')

        # Store and return the strategy dict
        self.strategy = get_strategy_details(strategy, time=self.time)
        return self.strategy


class GWRetractionEvent(Event):
    """A class to represent a Gravitational Wave Retraction alert."""

    VALID_PACKET_TYPES = {
        164: 'LVC_RETRACTION',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_type not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_type} not valid for this event class')
        self.notice = self.VALID_PACKET_TYPES[self.packet_type]

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)

        # Basic attributes
        self.type = 'GW_RETRACTION'
        self.source = 'LVC'
        self.id = top_params['GraceID']['value']  # e.g. S190510g
        self.name = '{}_{}'.format(self.source, self.id)  # e.g. LVC_S190510g

        # Get info from the VOEvent
        # Retractions have far fewer params
        self.gracedb_url = top_params['EventPage']['value']

    def get_skymap(self):
        """Return None."""
        return

    def get_strategy(self):
        """Return None."""
        return


class GRBEvent(Event):
    """A class to represent a Gamma-Ray Burst Event."""

    VALID_PACKET_TYPES = {
        115: 'FERMI_GBM_FIN_POS',
        61: 'SWIFT_BAT_GRB_POS',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_type not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_type} not valid for this event class')
        self.notice = self.VALID_PACKET_TYPES[self.packet_type]

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Basic attributes
        self.type = 'GRB'
        self.source = self.notice.split('_')[0].capitalize()
        self.id = top_params['TrigID']['value']  # e.g. 579943502
        self.name = '{}_{}'.format(self.source, self.id)  # e.g. Fermi_579943502

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
            self.systematic_error = Angle(5.6, unit='deg')
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

    def get_strategy(self):
        """Get the event observing strategy."""
        if self.strategy:
            return self.strategy

        # Decide which strategy to use
        if self.source == 'Swift':
            strategy = 'GRB_SWIFT'
        elif self.source == 'Fermi':
            if self.duration.lower() == 'short':
                strategy = 'GRB_FERMI_SHORT'
            else:
                strategy = 'GRB_FERMI'
        else:
            raise ValueError(f'Cannot determine observing strategy for source "{self.source}"')

        # Store and return the strategy dict
        self.strategy = get_strategy_details(strategy, time=self.time)
        return self.strategy


class NUEvent(Event):
    """A class to represent a Neutrino (NU) Event."""

    VALID_PACKET_TYPES = {
        173: 'ICECUBE_ASTROTRACK_GOLD',
        174: 'ICECUBE_ASTROTRACK_BRONZE',
        176: 'ICECUBE_CASCADE',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_type not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_type} not valid for this event class')
        self.notice = self.VALID_PACKET_TYPES[self.packet_type]

        # Get XML param dicts
        # NB: you can't store these on the Event because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        # group_params = vp.get_grouped_params(self.voevent)

        # Default params
        self.type = 'NU'
        self.source = 'IceCube'
        self.id = top_params['AMON_ID']['value']  # e.g. 13311922683750
        self.name = '{}_{}'.format(self.source, self.id)  # e.g. IceCube_13311922683750

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

    def get_strategy(self):
        """Get the event observing strategy."""
        if self.strategy:
            return self.strategy

        # Decide which strategy to use
        if self.notice == 'ICECUBE_ASTROTRACK_GOLD':
            strategy = 'NU_ICECUBE_GOLD'
        elif self.notice == 'ICECUBE_ASTROTRACK_BRONZE':
            strategy = 'NU_ICECUBE_BRONZE'
        elif self.notice == 'ICECUBE_CASCADE':
            strategy = 'NU_ICECUBE_CASCADE'
        else:
            raise ValueError(f'Cannot determine observing strategy for notice "{self.notice}"')

        # Store and return the strategy dict
        self.strategy = get_strategy_details(strategy, time=self.time)
        return self.strategy
