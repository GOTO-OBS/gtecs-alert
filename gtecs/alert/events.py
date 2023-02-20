"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus
from urllib.request import urlopen

import astropy.units as u
from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time
from astropy.utils.data import download_file

from gototile.skymap import SkyMap

import numpy as np

import voeventdb.remote.apiv1 as vdb

import voeventparse as vp

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
        try:
            self.voevent = vp.loads(payload)
        except Exception as err:
            raise ValueError('Invalid payload') from err

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

        # Key attributes (unknowns will be overwritten by subclasses)
        self.packet_type = self._get_packet_type(payload)
        self.role = self.voevent.attrib['role']
        try:
            self.time = Time(vp.convenience.get_event_time_as_utc(self.voevent, index=0))
        except Exception:
            # Some test events don't have times
            self.time = None
        self.notice = 'unknown'
        self.notice_time = Time(str(self.voevent.Who.Date))
        self.author = str(self.voevent.Who.Author.contactName)
        try:
            self.contact = str(self.voevent.Who.Author.contactEmail)
        except AttributeError:
            self.contact = None
        self.type = 'unknown'
        self.source = 'unknown'
        self.coord = None
        self.skymap = None
        self.skymap_url = None

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

    def save(self, path):
        """Save this event to a file in the given directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        out_path = os.path.join(path, filename)
        with open(out_path, 'wb') as f:
            f.write(self.payload)
        return out_path

    def get_skymap(self, nside=128):
        """Return the Event skymap as a `gototile.skymap.SkyMap object."""
        if self.skymap is not None:
            # Don't do anything if the skymap has already been downloaded/created
            return self.skymap

        # Try to download the skymap from the Event URL
        if self.skymap_url is not None:
            try:
                # The file gets stored in /tmp/
                # Don't cache, force redownload every time
                # https://github.com/GOTO-OBS/goto-alert/issues/36
                self.skymap_file = download_file(self.skymap_url, cache=False)
                self.skymap = SkyMap.from_fits(self.skymap_file)
            except Exception:
                # Some error meant we can't download the skymap
                # So instead we'll try and create our own
                pass

        # If the Event has coordinates then create a Gaussian skymap
        if self.skymap is None and self.coord is not None:
            self.skymap = SkyMap.from_position(
                self.coord.ra.deg,
                self.coord.dec.deg,
                self.total_error.deg,
                nside=nside,
            )

        return self.skymap

    @property
    def strategy(self):
        """Get the event observing strategy key."""
        return 'DEFAULT'

    @property
    def strategy_dict(self):
        """Get the event observing strategy details."""
        return get_strategy_details(self.strategy)

    def get_details(self):
        """Get a list of the event details for Slack messages."""
        details = [
            f'IVORN: {self.ivorn}',
            f'Event time: {self.time.iso}'
            f' _({(Time.now() - self.time).to(u.hour).value:.1f}h ago)_',
            f'Notice time: {self.notice_time.iso}'
            f' _({(Time.now() - self.notice_time).to(u.hour).value:.1f}h ago)_',
        ]
        return details


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
            try:
                classification_group = group_params['Classification']
            except KeyError:
                # Fallback for older events that weren't keyed properly
                for _, group_dict in group_params.allitems():
                    if 'BNS' in group_dict:
                        classification_group = group_dict
            self.classification = {key: float(classification_group[key]['value'])
                                   for key in classification_group}

            try:
                properties_group = group_params['Properties']
            except KeyError:
                for _, group_dict in group_params.allitems():
                    if 'HasNS' in group_dict:
                        properties_group = group_dict
            self.properties = {key: float(properties_group[key]['value'])
                               for key in properties_group}
        else:
            self.classification = {}
            self.properties = {}

        # Get skymap URL
        try:
            skymap_group = group_params['GW_SKYMAP']
        except KeyError:
            for _, group_dict in group_params.allitems():
                if 'skymap_fits' in group_dict:
                    skymap_group = group_dict
        self.skymap_url = skymap_group['skymap_fits']['value']

    @property
    def strategy(self):
        """Get the event observing strategy key."""
        if self.skymap is None:
            # This is very annoying, but we need to get the skymap to get the distance
            # TODO: We could assume it is far, would that be better?
            raise ValueError('Cannot determine strategy without skymap')

        if self.group == 'CBC':
            if self.properties['HasNS'] > 0.25:
                if self.skymap.header['distmean'] < 400:
                    return 'GW_CLOSE_NS'
                else:
                    return 'GW_FAR_NS'
            else:
                if self.skymap.header['distmean'] < 100:
                    return 'GW_CLOSE_BH'
                else:
                    return 'GW_FAR_BH'
        elif self.group == 'Burst':
            return 'GW_BURST'
        else:
            raise ValueError(f'Cannot determine observing strategy for group "{self.group}"')

    def get_details(self):
        """Get a list of the event details for Slack messages."""
        details = [
            f'IVORN: {self.ivorn}',
            f'Event time: {self.time.iso}'
            f' _({(Time.now() - self.time).to(u.hour).value:.1f}h ago)_',
            f'Notice time: {self.notice_time.iso}'
            f' _({(Time.now() - self.notice_time).to(u.hour).value:.1f}h ago)_',
        ]
        # Add event properties
        details += [
            f'Group: {self.group}',
            f'FAR: ~1 per {1 / self.far / 3.154e+7:.1f} yrs',
        ]
        # Add skymap info only if we have downloaded the skymap
        if self.skymap is not None:
            distance = self.skymap.header['distmean']
            distance_error = self.skymap.header['diststd']
            area = self.skymap.get_contour_area(0.9)
            details += [
                f'Distance: {distance:.0f}+/-{distance_error:.0f} Mpc',
                f'90% probability area: {area:.0f} sq deg'
            ]
        else:
            details += [
                'Distance: UNKNOWN',
            ]
        # Add classification info for CBC events
        if self.group == 'CBC':
            sorted_class = sorted(
                self.classification.keys(),
                key=lambda key: self.classification[key],
                reverse=True,
            )
            class_list = [
                f'{key}:{self.classification[key]:.1%}'
                for key in sorted_class
                if self.classification[key] > 0.0005
            ]
            details += [
                f'Classification: {", ".join(class_list)}',
                f'HasNS (if real): {self.properties["HasNS"]:.0%}',
            ]
        details += [
            f'GraceDB page: {self.gracedb_url}',
        ]

        return details


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

    @property
    def strategy(self):
        """Get the event observing strategy key."""
        return None  # Retractions don't have an observing strategy

    def get_details(self):
        """Get a list of the event details for Slack messages."""
        details = [
            f'IVORN: {self.ivorn}',
            f'Event time: {self.time.iso}'
            f' _({(Time.now() - self.time).to(u.hour).value:.1f}h ago)_',
            f'Notice time: {self.notice_time.iso}'
            f' _({(Time.now() - self.notice_time).to(u.hour).value:.1f}h ago)_',
        ]
        # Nothing much to add, just note clearly it's a retraction event
        details += [
            f'GraceDB page: {self.gracedb_url}',
            f'*THIS IS A RETRACTION OF EVENT {self.id}*',
        ]

        return details


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

        # Position coordinates & error
        position = vp.get_event_position(self.voevent)
        self.coord = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
        self.coord_error = Angle(position.err, unit=position.units)
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

    @property
    def strategy(self):
        """Get the event observing strategy key."""
        if self.source == 'Swift':
            return 'GRB_SWIFT'
        elif self.source == 'Fermi':
            if self.duration.lower() == 'short':
                return 'GRB_FERMI_SHORT'
            else:
                return 'GRB_FERMI'
        else:
            raise ValueError(f'Cannot determine observing strategy for source "{self.source}"')

    def get_details(self):
        """Get a list of the event details for Slack messages."""
        details = [
            f'IVORN: {self.ivorn}',
            f'Event time: {self.time.iso}'
            f' _({(Time.now() - self.time).to(u.hour).value:.1f}h ago)_',
            f'Notice time: {self.notice_time.iso}'
            f' _({(Time.now() - self.notice_time).to(u.hour).value:.1f}h ago)_',
        ]
        # Add event location
        details += [
            f'Position: {self.coord.to_string("hmsdms")} ({self.coord.to_string()})',
            f'Position error: {self.total_error:.3f}',
        ]
        if self.source == 'Fermi':
            # Add duration (long/short) for Fermi events
            details += [
                f'Duration: {self.duration.capitalize()}',
            ]

        return details


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

        # Position coordinates & error
        position = vp.get_event_position(self.voevent)
        self.coord = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
        self.coord_error = Angle(position.err, unit=position.units)
        if self.notice == 'ICECUBE_CASCADE':
            # Systematic error for cascade event is given, so = 0
            self.systematic_error = Angle(0, unit='deg')
        else:
            self.systematic_error = Angle(.2, unit='deg')
        self.total_error = Angle(np.sqrt(self.coord_error ** 2 + self.systematic_error ** 2),
                                 unit='deg')

        # Get skymap URL
        if 'skymap_fits' in top_params:
            self.skymap_url = top_params['skymap_fits']['value']
        else:
            self.skymap_url = None

    @property
    def strategy(self):
        """Get the event observing strategy key."""
        if self.notice == 'ICECUBE_ASTROTRACK_GOLD':
            return 'NU_ICECUBE_GOLD'
        elif self.notice == 'ICECUBE_ASTROTRACK_BRONZE':
            return 'NU_ICECUBE_BRONZE'
        elif self.notice == 'ICECUBE_CASCADE':
            return 'NU_ICECUBE_CASCADE'
        else:
            raise ValueError(f'Cannot determine observing strategy for notice "{self.notice}"')

    def get_details(self):
        """Get a list of the event details for Slack messages."""
        details = [
            f'IVORN: {self.ivorn}',
            f'Event time: {self.time.iso}'
            f' _({(Time.now() - self.time).to(u.hour).value:.1f}h ago)_',
            f'Notice time: {self.notice_time.iso}'
            f' _({(Time.now() - self.notice_time).to(u.hour).value:.1f}h ago)_',
        ]
        # Add event properties
        details += [
            f'Signalness: {self.signalness:.0%} probability to be astrophysical in origin',
            f'FAR: ~1 per {1 / self.far:.1f} yrs',
        ]
        # Add event location
        details += [
            f'Position: {self.coord.to_string("hmsdms")} ({self.coord.to_string()})',
            f'Position error: {self.total_error:.3f}',
        ]

        return details
