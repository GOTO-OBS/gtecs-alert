"""Event classes to contain VOEvents."""

import os
from urllib.parse import quote_plus
from urllib.request import urlopen

from astroplan import FixedTarget

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

        # Key attributes
        self.packet_type = self._get_packet_type(payload)
        self.role = self.voevent.attrib['role']
        try:
            self.time = Time(vp.convenience.get_event_time_as_utc(self.voevent, index=0))
        except Exception:
            # Some test events don't have times
            self.time = None
        self.notice_time = Time(str(self.voevent.Who.Date))
        self.author = str(self.voevent.Who.Author.contactName)
        try:
            self.contact = str(self.voevent.Who.Author.contactEmail)
        except AttributeError:
            self.contact = None

        # Set default attributes
        # Any subclass will overwrite these
        self.notice = 'unknown'
        self.type = 'unknown'
        self.source = 'unknown'
        self.position = None
        self.coord = None
        self.target = None
        self.skymap = None
        self.properties = {}
        self.strategy = None
        self.grid = None
        self.tiles = None

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

    def get_tiles(self, grid, selection_contour=None):
        """Apply the Event skymap to the given grid and return a table of filtered tiles."""
        if self.tiles is None:
            # Apply the Event skymap to the grid
            if self.skymap is None:
                self.get_skymap()
            if self.skymap is None:
                # Can't get tiles if there's no skymap!
                return
            self.grid = grid
            self.grid.apply_skymap(self.skymap)

            # Sort and store the full table of tiles on the Event
            self.tiles = self.grid.get_table()

        # If no selection or strategy then just return the full table
        # TODO: I hate having to get the strategy like this, it's just because of the GW distances
        if self.strategy is None:
            self.get_strategy()
        if self.strategy is None or selection_contour is None:
            return self.tiles

        # Limit tiles to add to the database
        # First select only tiles covering the given contour level
        mask = self.grid.contours < selection_contour

        # Then limit the number of tiles, if given
        if self.strategy['tile_limit'] is not None and sum(mask) > self.strategy['tile_limit']:
            # Limit by probability above `tile_limit`th tile
            min_tile_prob = sorted(self.grid.probs, reverse=True)[self.strategy['tile_limit']]
            mask &= self.grid.probs > min_tile_prob

        # Finally limit to tiles which contain more than a given probability
        if self.strategy['prob_limit'] is not None:
            mask &= self.grid.probs > self.strategy['prob_limit']

        # Return the masked table
        return self.tiles[mask]

    def get_strategy(self):
        """Return default strategy."""
        self.strategy = get_strategy_details()
        return self.strategy

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
        if self.distance == np.inf:
            details += [
                'Distance: UNKNOWN',
            ]
        else:
            details += [
                f'Distance: {self.distance:.0f}+/-{self.distance_error:.0f} Mpc',
                f'90% probability area: {self.contour_areas[0.9]:.0f} sq deg'
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

    def get_skymap(self):
        """Return None."""
        return

    def get_strategy(self):
        """Return None."""
        return

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
