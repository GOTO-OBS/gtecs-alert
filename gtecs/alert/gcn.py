"""Classes to represent GCN alert notices."""

import os
from urllib.parse import quote_plus
from urllib.request import urlopen

from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time
from astropy.utils.data import download_file

from gototile.skymap import SkyMap

import numpy as np

import voeventdb.remote.apiv1 as vdb

import voeventparse as vp

from .strategy import get_strategy_details


class GCNNotice:
    """A class to represent a single GCN notice using the VOEvent protocol.

    Some notices are better represented as one of the more specialised subclasses.

    Use one of the following classmethods to to create the appropriate class:
        - GCNNotice.from_file()
        - GCNNotice.from_url()
        - GCNNotice.from_ivorn()
        - GCNNotice.from_payload()
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
        self.source = 'GCN'
        top_params = vp.get_toplevel_params(self.voevent)
        self.packet_id = int(top_params['Packet_Type']['value'])
        self.packet_type = 'unknown'  # Matched to ID in subclasses
        self.role = self.voevent.attrib['role']
        self.time = Time(str(self.voevent.Who.Date))
        self.author = str(self.voevent.Who.Author.contactName)
        try:
            self.contact = str(self.voevent.Who.Author.contactEmail)
        except AttributeError:
            self.contact = None

        # Event properties (will mostly be filled by subclasses)
        self.event_name = None
        self.event_id = None
        try:
            self.event_time = Time(vp.convenience.get_event_time_as_utc(self.voevent, index=0))
        except Exception:
            # Some test events don't have times
            self.event_time = None
        self.event_type = 'unknown'
        self.event_source = 'unknown'
        self.position = None
        self.position_error = None
        self.skymap = None
        self.skymap_url = None
        self.skymap_file = None

    def __repr__(self):
        return '{}(ivorn={})'.format(self.__class__.__name__, self.ivorn)

    @staticmethod
    def _get_class(payload):
        """Get the correct class of notice by trying each subclass."""
        subclasses = [GWNotice, GWRetractionNotice, GRBNotice, NUNotice]
        for subclass in subclasses:
            try:
                return subclass(payload)
            except ValueError:
                pass
        return GCNNotice(payload)

    @classmethod
    def from_payload(cls, payload):
        """Create a GCNNotice (or subclass) from a VOEvent payload."""
        notice = cls._get_class(payload)
        if cls != GCNNotice and cls != notice.__class__:
            raise ValueError('Subtype mismatch (`{}` detected)'.format(
                             notice.__class__.__name__
                             ))
        return notice

    @classmethod
    def from_ivorn(cls, ivorn):
        """Create a GCNNotice (or subclass) by querying the 4pisky VOEvent database."""
        payload = vdb.packet_xml(ivorn)
        return cls.from_payload(payload)

    @classmethod
    def from_url(cls, url):
        """Create a GCNNotice (or subclass) by downloading the VOEvent XML from the given URL."""
        with urlopen(url) as r:
            payload = r.read()
        return cls.from_payload(payload)

    @classmethod
    def from_file(cls, filepath):
        """Create a GCNNotice (or subclass) by reading a VOEvent XML file."""
        with open(filepath, 'rb') as f:
            payload = f.read()
        return cls.from_payload(payload)

    def save(self, path):
        """Save this notice to a file in the given directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        out_path = os.path.join(path, filename)
        with open(out_path, 'wb') as f:
            f.write(self.payload)
        return out_path

    def get_skymap(self, nside=128, timeout=60):
        """Return the skymap as a `gototile.skymap.SkyMap object."""
        if self.skymap is not None:
            # Don't do anything if the skymap has already been downloaded/created
            return self.skymap

        # Try to download the skymap from a given URL
        if self.skymap_url is not None:
            try:
                # The file gets stored in /tmp/
                # Don't cache, force redownload every time
                # https://github.com/GOTO-OBS/goto-alert/issues/36
                try:
                    skymap_file = download_file(self.skymap_url, cache=False, timeout=timeout)
                except Exception:
                    # Maybe it's a local file?
                    skymap_file = self.skymap_url
                self.skymap = SkyMap.from_fits(skymap_file)
                self.skymap_file = skymap_file
            except Exception:
                # Some error meant we can't download the skymap
                # If we have a position we can try and create our own
                if self.position is not None:
                    pass
                else:
                    raise

        # If the notice includes coordinates then create a Gaussian skymap
        # This can also be used as a fallback if the skymap download fails
        if self.skymap is None and self.position is not None:
            self.skymap = SkyMap.from_position(
                self.position.ra.deg,
                self.position.dec.deg,
                self.position_error.deg,
                nside=nside,
            )
            self.skymap_file = None

        return self.skymap

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'DEFAULT'

    @property
    def strategy_dict(self):
        """Get the observing strategy details."""
        return get_strategy_details(self.strategy, time=self.event_time)

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'
        return text


class GWNotice(GCNNotice):
    """A class to represent a Gravitational Wave detection notice."""

    VALID_PACKET_TYPES = {
        163: 'LVC_EARLY_WARNING',
        150: 'LVC_PRELIMINARY',
        151: 'LVC_INITIAL',
        152: 'LVC_UPDATE',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_id not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_id} not valid for this class')
        self.packet_type = self.VALID_PACKET_TYPES[self.packet_id]
        self.event_type = 'GW'
        self.event_source = 'LVC'

        # Get XML param dicts
        # NB: you can't store these on the class because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Get info from the VOEvent
        # See https://emfollow.docs.ligo.org/userguide/content.html#notice-contents
        self.event_id = top_params['GraceID']['value']  # e.g. S190510g
        self.event_name = '{}_{}'.format(self.event_source, self.event_id)  # e.g. LVC_S190510g
        self.gracedb_url = top_params['EventPage']['value']
        self.instruments = top_params['Instruments']['value']
        self.group = top_params['Group']['value']  # CBC or Burst
        self.pipeline = top_params['Pipeline']['value']
        self.far = float(top_params['FAR']['value'])  # In Hz
        try:
            self.significant = bool(top_params['Significant']['value'])
        except KeyError:
            # Fallback for older notices that didn't include the significance
            # This uses the "official" definition of 1/month for CBC and 1/year for bursts,
            # see https://emfollow.docs.ligo.org/userguide/analysis/index.html#alert-threshold
            if self.group == 'CBC' and self.far < 12 / (60 * 60 * 24 * 365):
                self.significant = True
            elif self.group == 'Burst' and self.far < 1 / (60 * 60 * 24 * 365):
                self.significant = True
            else:
                self.significant = False

        # Get classification probabilities and properties
        if self.group == 'CBC':
            try:
                classification_group = group_params['Classification']
            except KeyError:
                # Fallback for older notices that weren't keyed properly
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
            self.classification = None
            self.properties = None

        # Get skymap URL
        try:
            skymap_group = group_params['GW_SKYMAP']
        except KeyError:
            for _, group_dict in group_params.allitems():
                if 'skymap_fits' in group_dict:
                    skymap_group = group_dict
        self.skymap_url = skymap_group['skymap_fits']['value']

        # Get external coincidence, if any
        try:
            external_group = group_params['External Coincidence']
            self.external = {
                'gcn_notice_id': int(external_group['External_GCN_Notice_Id']['value']),
                'ivorn': external_group['External_Ivorn']['value'],
                'observatory': external_group['External_Observatory']['value'],
                'search': external_group['External_Search']['value'],
                'time_difference': float(external_group['Time_Difference']['value']),
                'time_coincidence_far': float(external_group['Time_Coincidence_FAR']['value']),
                'time_sky_position_coincidence_far':
                    float(external_group['Time_Sky_Position_Coincidence_FAR']['value']),
                'combined_skymap_url': external_group['joint_skymap_fits']['value'],
                }
        except KeyError:
            self.external = None

    @property
    def strategy(self):
        """Get the observing strategy key."""
        if self.skymap is None:
            # This is very annoying, but we need to get the skymap to get the distance
            # TODO: We could assume it is far, would that be better?
            raise ValueError('Cannot determine strategy without skymap')

        if self.group == 'CBC':
            if self.properties['HasRemnant'] > 0.25:
                if (self.skymap.get_contour_area(0.9) < 5000 and
                        self.skymap.header['distmean'] < 200):
                    if self.skymap.get_contour_area(0.9) < 1000:
                        return 'GW_RANK_1_NARROW'
                    else:
                        return 'GW_RANK_1_WIDE'
                else:
                    if self.skymap.get_contour_area(0.9) < 1000:
                        return 'GW_RANK_2_NARROW'
                    else:
                        return 'GW_RANK_2_WIDE'
            else:
                if self.significant and self.skymap.header['distmean'] < 200:
                    if self.skymap.get_contour_area(0.9) < 1000:
                        return 'GW_RANK_4_NARROW'
                    else:
                        return 'GW_RANK_4_WIDE'
                else:
                    return 'IGNORE'

        elif self.group == 'Burst':
            if self.significant and self.skymap.get_contour_area(0.9) < 5000:
                if self.skymap.get_contour_area(0.9) < 1000:
                    return 'GW_RANK_3_NARROW'
                else:
                    return 'GW_RANK_3_WIDE'
            else:
                return 'IGNORE'

        else:
            raise ValueError(f'Cannot determine observing strategy for group "{self.group}"')

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'
        text += f'Pipeline: {self.pipeline}\n'
        text += f'Instruments: {self.instruments}\n'
        text += f'GraceDB page: {self.gracedb_url}\n'

        # Classification info
        far_years = self.far * 60 * 60 * 24 * 360  # convert from /s to /yr
        if far_years > 1:
            text += f'FAR: ~{far_years:.0f} per year'
        else:
            text += f'FAR: ~1 per {1 / far_years:.1f} years'
        if self.significant:
            text += ' (significant=True)\n'
        else:
            text += ' (significant=False)\n'
        text += f'Group: {self.group}\n'
        if self.classification is not None:
            sorted_classification = sorted(
                self.classification.keys(),
                key=lambda key: self.classification[key],
                reverse=True,
            )
            class_list = [
                f'{key}:{self.classification[key]:.1%}'
                for key in sorted_classification
                if self.classification[key] > 0.0005
            ]
            text += f'Classification: {", ".join(class_list)}\n'
        elif self.group == 'Burst':
            # Burst events aren't classified
            text += 'Classification: N/A\n'
        else:
            text += 'Classification: *UNKNOWN*\n'
        if self.properties is not None:
            text += f'HasNS: {self.properties["HasNS"]:.0%}\n'
            try:
                text += f'HasRemnant: {self.properties["HasRemnant"]:.0%}\n'
            except KeyError:
                pass

        # Skymap info (only if we have downloaded the skymap)
        if self.skymap is not None:
            if 'distmean' in self.skymap.header:
                distance = self.skymap.header['distmean']
                distance_error = self.skymap.header['diststd']
                text += f'Distance: {distance:.0f}+/-{distance_error:.0f} Mpc\n'
            elif self.group == 'Burst':
                # We don't expect a distance for burst events
                text += 'Distance: N/A\n'
            else:
                text += 'Distance: *UNKNOWN*\n'
            area = self.skymap.get_contour_area(0.9)
            text += f'90% probability area: {area:.0f} sq deg\n'
        else:
            text += '*NO SKYMAP FOUND*\n'

        # Coincidence info
        if self.external is not None:
            text += '\n'
            text += '*External event coincidence detected!*\n'
            text += f'Source: {self.external["observatory"]}\n'
            text += f'IVORN: {self.external["ivorn"]}\n'
            far_years = self.external['time_sky_position_coincidence_far'] * 60 * 60 * 24 * 360
            if far_years > 1:
                text += f'FAR: ~{far_years:.0f} per year\n'
            else:
                text += f'FAR: ~1 per {1 / far_years:.1f} years\n'

        return text


class GWRetractionNotice(GCNNotice):
    """A class to represent a Gravitational Wave retraction notice."""

    VALID_PACKET_TYPES = {
        164: 'LVC_RETRACTION',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_id not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_id} not valid for this class')
        self.packet_type = self.VALID_PACKET_TYPES[self.packet_id]
        self.event_type = 'GW'
        self.event_source = 'LVC'

        # Get XML param dicts
        # NB: you can't store these on the class because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)

        # Get info from the VOEvent
        self.event_id = top_params['GraceID']['value']  # e.g. S190510g
        self.event_name = '{}_{}'.format(self.event_source, self.event_id)  # e.g. LVC_S190510g
        self.gracedb_url = top_params['EventPage']['value']

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'RETRACTION'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'
        text += f'GraceDB page: {self.gracedb_url}\n'

        text += f'*THIS IS A RETRACTION OF EVENT {self.event_name}*\n'

        return text


class GRBNotice(GCNNotice):
    """A class to represent a Gamma-Ray Burst detection notice."""

    VALID_PACKET_TYPES = {
        115: 'FERMI_GBM_FIN_POS',
        61: 'SWIFT_BAT_GRB_POS',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_id not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_id} not valid for this class')
        self.packet_type = self.VALID_PACKET_TYPES[self.packet_id]
        self.event_type = 'GRB'
        self.event_source = self.packet_type.split('_')[0].capitalize()

        # Get XML param dicts
        # NB: you can't store these on the class because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)
        group_params = vp.get_grouped_params(self.voevent)

        # Get info from the VOEvent
        self.event_id = top_params['TrigID']['value']  # e.g. 579943502
        self.event_name = '{}_{}'.format(self.event_source, self.event_id)  # e.g. Fermi_579943502
        if self.event_source == 'Fermi':
            self.properties = {key: group_params['Trigger_ID'][key]['value']
                               for key in group_params['Trigger_ID']
                               if key != 'Long_short'}
            try:
                self.duration = group_params['Trigger_ID']['Long_short']['value']
            except KeyError:
                # Some don't have the duration
                self.duration = 'unknown'
        elif self.event_source == 'Swift':
            self.properties = {key: group_params['Solution_Status'][key]['value']
                               for key in group_params['Solution_Status']}
        else:
            raise ValueError(f'Unknown GRB source {self.event_source}')
        for key in self.properties:
            if self.properties[key] == 'true':
                self.properties[key] = True
            elif self.properties[key] == 'false':
                self.properties[key] = False

        # Position coordinates & error
        position = vp.get_event_position(self.voevent)
        self.position = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
        self.position_error = Angle(position.err, unit=position.units)
        if self.event_source == 'Fermi':
            systematic_error = Angle(5.6, unit='deg')
            self.position_error = Angle(np.sqrt(self.position_error ** 2 + systematic_error ** 2),
                                        unit='deg')

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
        """Get the observing strategy key."""
        if self.event_source == 'Swift':
            return 'GRB_SWIFT'
        elif self.event_source == 'Fermi':
            if self.duration.lower() == 'short':
                return 'GRB_FERMI_SHORT'
            else:
                return 'GRB_FERMI'
        else:
            raise ValueError(f'Unknown GRB source: "{self.event_source}"')

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Classification info
        if self.event_source == 'Fermi':
            text += f'Duration: {self.duration.capitalize()}\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text


class NUNotice(GCNNotice):
    """A class to represent a Neutrino (NU) detection notice."""

    VALID_PACKET_TYPES = {
        173: 'ICECUBE_ASTROTRACK_GOLD',
        174: 'ICECUBE_ASTROTRACK_BRONZE',
        176: 'ICECUBE_CASCADE',
    }

    def __init__(self, payload):
        super().__init__(payload)
        if self.packet_id not in self.VALID_PACKET_TYPES:
            raise ValueError(f'GCN packet type {self.packet_id} not valid for this class')
        self.packet_type = self.VALID_PACKET_TYPES[self.packet_id]
        self.event_type = 'NU'
        self.event_source = 'IceCube'

        # Get XML param dicts
        # NB: you can't store these on the class because they're unpickleable.
        top_params = vp.get_toplevel_params(self.voevent)

        # Get info from the VOEvent
        self.event_id = top_params['AMON_ID']['value']  # e.g. 13311922683750
        self.event_name = '{}_{}'.format(self.event_source, self.event_id)  # e.g. IceCube_133...
        self.signalness = float(top_params['signalness']['value'])
        self.far = float(top_params['FAR']['value'])

        # Position coordinates & error
        position = vp.get_event_position(self.voevent)
        self.position = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
        self.position_error = Angle(position.err, unit=position.units)
        if self.packet_type != 'ICECUBE_CASCADE':
            # Systematic error for cascade events is 0
            systematic_error = Angle(0.2, unit='deg')
            self.position_error = Angle(np.sqrt(self.position_error ** 2 + systematic_error ** 2),
                                        unit='deg')

        # Get skymap URL
        if 'skymap_fits' in top_params:
            self.skymap_url = top_params['skymap_fits']['value']
        else:
            self.skymap_url = None

    @property
    def strategy(self):
        """Get the observing strategy key."""
        if self.packet_type == 'ICECUBE_ASTROTRACK_GOLD':
            return 'NU_ICECUBE_GOLD'
        elif self.packet_type == 'ICECUBE_ASTROTRACK_BRONZE':
            return 'NU_ICECUBE_BRONZE'
        elif self.packet_type == 'ICECUBE_CASCADE':
            return 'NU_ICECUBE_CASCADE'
        else:
            raise ValueError(f'Cannot determine observing strategy for "{self.packet_type}" notice')

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Classification info
        text += f'Signalness: {self.signalness:.0%} probability to be astrophysical in origin\n'
        text += f'FAR: ~1 per {1 / self.far:.1f} yrs\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text
