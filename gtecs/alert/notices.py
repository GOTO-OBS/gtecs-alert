"""Classes to represent transient alert notices."""

import importlib.resources
import json
import os
import re
import xml
from base64 import b64decode
from collections import Counter
from urllib.parse import quote_plus
from urllib.request import urlopen

import astropy.units as u
from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time
from astropy.utils.data import download_file

from gototile.skymap import SkyMap

from hop.models import AvroBlob, JSONBlob, VOEvent

import numpy as np

import requests

import voeventdb.remote.apiv1 as vdb


# Load the strategy definitions
with open(importlib.resources.files('gtecs.alert.data').joinpath('strategies.json')) as f:
    STRATEGIES = json.load(f)


def deserialize(raw_payload):
    """Deserialize a raw payload to a hop model class.

    While hop-client does have a deserialize function, it only works with the messages
    which contain format information. Here we only have the raw payload, so we need to
    try a few different formats to see which one works.

    Valid formats:
    - VOEvent XML (produced by GCN Classic notices) -> hop.models.VOEvent
    - VOEvent JSON (GCN Notices converted to JSON by SCIMMA) -> hop.models.VOEvent
    - Avro (newer encoding format used by IGWN notices) -> hop.models.AvroBlob
    - Pure JSON (e.g. new GCN Unified schema) -> hop.models.JSONBlob
    """
    if isinstance(raw_payload, str):
        raw_payload = raw_payload.encode('utf-8')

    # Try Avro first, since it's the most specific
    try:
        return AvroBlob.deserialize(raw_payload)
    except TypeError:
        pass
    except ValueError as err:
        if 'is it an avro file?' in str(err):
            pass
        else:
            raise

    # If it's valid JSON it might be a VOEvent, or else a generic JSONBlob
    try:
        return VOEvent.deserialize(raw_payload)
    except TypeError:
        # Valid JSON, but not a VOEvent
        try:
            return JSONBlob.deserialize(raw_payload)
        except json.JSONDecodeError:
            pass
    except json.JSONDecodeError:
        pass  # We'll try XML parsing instead

    # If it's not Avro or JSON, try parsing it as an XML VOEvent
    try:
        return VOEvent.load(raw_payload)
    except xml.parsers.expat.ExpatError:
        pass

    # No valid format found
    raise ValueError('Could not parse message as Avro, JSON or XML')


class InvalidNoticeError(Exception):
    """Exception raised for invalid notice types."""

    pass


class Notice:
    """A class to represent a single transient alert notice.

    Some notices are better represented as one of the more specialised subclasses.

    Use one of the following classmethods to to create the appropriate class:
        - Notice.from_file(filepath)
        - Notice.from_url(url)
        - Notice.from_ivorn(ivorn)
        - Notice.from_payload(raw_payload)
    """

    def __init__(self, message):
        self.creation_time = Time.now()

        # Store the message on the class
        if not isinstance(message, (AvroBlob, JSONBlob, VOEvent)):
            raise ValueError('Base message should be a hop.models message class')
        self.message = message
        self.payload = self.message.serialize()['content']
        if hasattr(message, 'content'):
            self.content = self.message.content
            if isinstance(self.message.content, list):
                # Avro messages are wrapped in a list for some reason
                if len(self.message.content) == 1:
                    self.content = self.message.content[0]
                else:
                    raise ValueError('Multiple contents found for message')
        else:
            # VOEvents don't store their raw content
            self.content = json.loads(self.payload)

        # Try to parse notice parameters for VOEvents
        if isinstance(self.message, VOEvent):
            # Frustratingly, the VOEvent schema allows multiple Params with the same name,
            # which makes parsing them just a bit more complicated...
            # It should never come up with GCN notices, so we'll just try making a dict
            # and raise an error if there are duplicates.
            # Plus there are grouped params, which may contain 0, 1 or more Params.
            # And again there can be multiple Params with the same name.

            # Top-level params
            if 'Param' not in self.content['What']:
                # No params (seems unlikely, but I think it's allowed)
                self.top_params = None
            elif isinstance(self.content['What']['Param'], dict):
                # Only one param
                self.top_params = {
                    k: v for k, v in self.content['What']['Param'].items() if k != 'name'}
            else:
                # Multiple params
                param_names = [p['name'] for p in self.content['What']['Param']]
                duplicates = [name for name, count in Counter(param_names).items() if count > 1]
                if duplicates:
                    raise ValueError(f'Duplicate Params found: {duplicates}')
                self.top_params = {
                    p['name']: {k: v for k, v in p.items() if k != 'name'}
                    for p in self.content['What']['Param']}

            # Grouped params
            self.group_params = {}
            if 'Group' in self.content['What']:
                if isinstance(self.content['What']['Group'], dict):
                    # only a single group, should be a list of
                    groups = [self.content['What']['Group']]
                else:
                    groups = self.content['What']['Group']
                for group in groups:
                    if 'name' not in group and 'type' in group:
                        # Some old (off-spec) GW notices didn't included group names, just types
                        group['name'] = group['type']
                    group_dict = {k: v for k, v in group.items() if k not in ['name', 'Param']}
                    if 'Param' not in group:
                        # No params (happens e.g. for GW Bursts - Classification & Properties)
                        self.group_params[group['name']] = group_dict
                    elif isinstance(group['Param'], dict):
                        # Only one param
                        group_dict[group['Param']['name']] = {
                            k: v for k, v in group['Param'].items()
                            if k != 'name'}
                        self.group_params[group['name']] = group_dict
                    else:
                        # Multiple params
                        param_names = [p['name'] for p in group['Param']]
                        duplicates = [
                            name for name, count in Counter(param_names).items()
                            if count > 1]
                        if duplicates:
                            msg = f'Duplicate Params found in group {group["name"]}: {duplicates}'
                            raise ValueError(msg)
                        for p in group['Param']:
                            group_dict[p['name']] = {k: v for k, v in p.items() if k != 'name'}
                        self.group_params[group['name']] = group_dict

        # Store and format IVORN
        # IVORNs are required for all VOEvents, but not all notices come from VOEvents.
        # We use the message IVORN as keys for all notices, so we have to make one up
        # for non-VOEvent messages.
        # TODO: Scrap IVORNs entirely, use source and event time to check uniqueness.
        if isinstance(self.message, VOEvent):
            self.ivorn = self.message.ivorn
        elif '$schema' in self.content:
            # It's a GCN using the Unified schema
            publisher, *rest = self.content['$schema'].split('/notices/')[-1].split('/')
            title = '_'.join(rest).strip('.schema.json')
            title += '_' + self.content['trigger_time']
            self.ivorn = f'ivo://nasa.gsfc.gcn/{publisher}#{title}'
        elif 'superevent_id' in self.content:
            # It's a new-style IGWN JSON notice
            # Sadly we can't recreate the old gwnet IVORNs because they don't include the
            # number of this notice. So we'll have to use the date instead, that should be
            # unique.
            event_id = self.content['superevent_id']
            notice_type = self.content['alert_type']
            notice_time = self.content['time_created']
            self.ivorn = f'ivo://gwnet/LVC#{event_id}_{notice_type}_{notice_time}'
        else:
            # Some other type we don't know the format for?
            self.ivorn = 'ivo://unknown/unknown#unknown'

        # Basic notice attributes
        if isinstance(self.message, VOEvent):
            self.source = self.message.ivorn.split('/')[3].split('#')[0]
            self.role = self.content['role']
            self.time = Time(self.content['Who']['Date'])
        elif '$schema' in self.content:
            self.source = self.content['$schema'].split('/notices/')[-1].split('/')[0]
            self.role = 'observation'  # TODO: remove roles, have .test = True/False
            self.time = Time(self.content['trigger_time'])
        elif 'superevent_id' in self.content:
            self.source = 'LVC'  # Backwards compatibility with GCNs, IGWN (or LVK) would be better
            self.role = 'observation'
            self.time = Time(self.content['time_created'])
        else:
            self.source = 'unknown'
            self.role = 'unknown'
            self.time = None

        # Event properties (filled by subclasses)
        self.type = 'unknown'  # e.g. INITIAL, OBSERVATION, RETRACTION
        self.event_type = 'unknown'  # e.g. GW, GRB, NU
        self.event_id = None  # e.g. S190510g, 579943502
        self.event_time = None
        self.position = None
        self.position_error = None
        self.skymap = None
        self.skymap_url = None
        self.skymap_file = None

    def __repr__(self):
        return '{}(ivorn={})'.format(self.__class__.__name__, self.ivorn)

    @staticmethod
    def _get_subclass(message):
        """Get the correct class of notice by trying each subclass."""
        base_notice = Notice(message)
        try:
            if base_notice.source.upper() == 'LVC':
                # We split retractions out into their own class
                if (hasattr(base_notice, 'top_params') and
                        'AlertType' in base_notice.top_params and
                        base_notice.top_params['AlertType']['value'].upper() == 'RETRACTION'):
                    return GWRetractionNotice(message)
                elif ('alert_type' in base_notice.content and
                        base_notice.content['alert_type'].upper() == 'RETRACTION'):
                    return GWRetractionNotice(message)
                else:
                    return GWNotice(message)
            elif base_notice.source.upper() == 'FERMI':
                return FermiNotice(message)
            elif base_notice.source.upper() == 'SWIFT':
                return SwiftNotice(message)
            elif base_notice.source.upper() == 'GECAM':
                return GECAMNotice(message)
            elif base_notice.source.upper() == 'EINSTEIN_PROBE':
                return EinsteinProbeNotice(message)
            elif base_notice.source.upper() == 'AMON':
                # AMON is the "Astrophysical Multimessenger Observatory Network",
                # and there are several different types of notices they produce.
                # For now we only care about the IceCube neutrino alerts.
                if hasattr(base_notice, 'ivorn') and 'ICECUBE' in base_notice.ivorn:
                    return IceCubeNotice(message)
        except InvalidNoticeError:
            # For whatever reason the notice isn't valid, so fall back to the default class
            pass
        return base_notice

    @classmethod
    def from_message(cls, message):
        """Create a Notice (or appropriate subclass) from a hop.models message class."""
        notice = cls._get_subclass(message)
        if cls != Notice and cls != notice.__class__:
            raise ValueError('Subtype mismatch (`{}` detected)'.format(
                             notice.__class__.__name__
                             ))
        return notice

    @classmethod
    def from_payload(cls, payload):
        """Create a Notice (or appropriate subclass) from a raw message payload."""
        # We need to try and deserialize the payload to get the correct message model
        message = deserialize(payload)
        return cls.from_message(message)

    @classmethod
    def from_ivorn(cls, ivorn):
        """Create a Notice (or appropriate subclass) by querying the 4pisky VOEvent database."""
        payload = vdb.packet_xml(ivorn)
        return cls.from_payload(payload)

    @classmethod
    def from_url(cls, url):
        """Create a Notice (or appropriate subclass) by downloading from the given URL."""
        with urlopen(url) as r:
            payload = r.read()
        return cls.from_payload(payload)

    @classmethod
    def from_file(cls, filepath):
        """Create a Notice (or appropriate subclass) from a file."""
        with open(filepath, 'rb') as f:
            payload = f.read()
        return cls.from_payload(payload)

    @property
    def event_name(self):
        """Get the event name string.

        This is a combination of "{notice.source}_{notice.event_id}",
        e.g. LVC_S190510g, Fermi_579943502.

        If an alert isn't given a unique ID, we'll use the event time as the identifier.
        """
        if self.event_id is not None:
            return f'{self.source}_{self.event_id}'
        elif self.event_time is not None:
            return f'{self.source}_{self.event_time.isot}'
        else:
            return f'{self.source}_<unknown>'

    def save(self, path):
        """Save this notice to a file in the given directory."""
        if not os.path.exists(path):
            os.mkdir(path)

        filename = quote_plus(self.ivorn)
        out_path = os.path.join(path, filename)
        with open(out_path, 'wb') as f:
            f.write(self.payload)
        return out_path

    def get_skymap(self, nside=128, **kwargs):
        """Return the skymap as a `gototile.skymap.SkyMap object."""
        if self.skymap is not None:
            # Don't do anything if the skymap has already been downloaded/created
            # This will also be true for IGWN alerts with embedded skymaps
            return self.skymap

        # Try to download the skymap from a given URL
        if self.skymap_url is not None:
            try:
                # The file gets stored in /tmp/
                # Don't cache, force redownload every time
                # https://github.com/GOTO-OBS/goto-alert/issues/36
                # Pass any other arguments (e.g. timeout, show_progress)
                try:
                    skymap_file = download_file(self.skymap_url, cache=False, **kwargs)
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

    @staticmethod
    def get_strategy_details(name='DEFAULT', time=None):
        """Get details of the requested strategy."""
        name = name.upper()
        if time is None:
            time = Time.now()

        if name in ['IGNORE', 'RETRACTION']:
            # Special cases
            return None

        # Get the correct strategy for the given key
        try:
            strategy_dict = STRATEGIES[name].copy()
        except KeyError as err:
            raise ValueError(f'Unknown strategy: {name}') from err

        # Check all the required keys are present
        if 'cadence' not in strategy_dict:
            raise ValueError(f'Undefined cadence for strategy {name}')
        if 'constraints' not in strategy_dict:
            raise ValueError(f'Undefined constraints for strategy {name}')
        if 'exposure_sets' not in strategy_dict:
            raise ValueError(f'Undefined exposure sets for strategy {name}')

        # Fill out the cadence strategy based on the given time
        # NB A list of multiple cadence strategies can be given, which makes this more awkward!
        # We assume subsequent cadences start after the previous one ends.
        if isinstance(strategy_dict['cadence'], dict):
            cadences = [strategy_dict['cadence']]
        else:
            cadences = strategy_dict['cadence']
        for i, cadence in enumerate(cadences):
            if i == 0:
                # Start the first one immediately
                cadence['start_time'] = time
            else:
                # Start the next one after the previous one ends
                cadence['start_time'] = cadences[i - 1]['start_time']
            if 'delay_hours' in strategy_dict:
                # Delay the start by the given time
                cadence['start_time'] += strategy_dict['delay_hours'] * u.hour
            cadence['stop_time'] = cadence['start_time'] + strategy_dict['valid_hours'] * u.hour
        if len(cadences) == 1:
            strategy_dict['cadence'] = cadences[0]
        else:
            strategy_dict['cadence'] = cadences

        return strategy_dict

    @property
    def strategy_dict(self):
        """Get the observing strategy details."""
        return self.get_strategy_details(self.strategy, time=self.event_time)

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'
        return text


class GWNotice(Notice):
    """A class to represent a Gravitational Wave detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'LVC':
            raise InvalidNoticeError(f'Invalid source for GW notice: "{self.source}"')

        # Event properties
        self.event_type = 'GW'
        if hasattr(self, 'top_params'):
            # Classic VOEvent format
            self.type = self.top_params['AlertType']['value'].upper()
            self.event_id = self.top_params['GraceID']['value']  # e.g. S190510g
            self.gracedb_url = self.top_params['EventPage']['value']
            self.instruments = self.top_params['Instruments']['value']
            self.group = self.top_params['Group']['value']  # CBC or Burst
            self.pipeline = self.top_params['Pipeline']['value']
            self.far = float(self.top_params['FAR']['value'])  # In Hz
            try:
                self.significant = self.top_params['Significant']['value'] == '1'
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
            if self.group == 'CBC':
                self.classification = {
                    k: float(v['value'])
                    for k, v in self.group_params['Classification'].items()
                    if 'value' in v}
                self.properties = {
                    k: float(v['value'])
                    for k, v in self.group_params['Properties'].items()
                    if 'value' in v}
            else:
                self.classification = None
                self.properties = None
            event_location = self.message.WhereWhen['ObsDataLocation']['ObservationLocation']
            event_time = event_location['AstroCoords']['Time']['TimeInstant']['ISOTime']
            self.event_time = Time(event_time)
            # Get the skymap URL to download later
            try:
                skymap_group = self.group_params['GW_SKYMAP']
            except KeyError as err:
                skymap_group = None
                # Some old notices used the name of the pipeline (e.g. bayestar) instead
                for group_name in self.group_params:
                    if self.group_params[group_name]['type'] == 'GW_SKYMAP':
                        skymap_group = self.group_params[group_name]
                        break
                if skymap_group is None:
                    raise ValueError('No skymap group found') from err
            self.skymap_url = skymap_group['skymap_fits']['value']
            # Get external coincidence, if any
            try:
                external_group = self.group_params['External Coincidence']
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
                # Override the skymap URL with the combined skymap
                self.skymap_url_original = self.skymap_url
                self.skymap_url = self.external['combined_skymap_url']
            except KeyError:
                self.external = None
        else:
            # New Kafka format
            self.type = self.content['alert_type'].upper()
            self.event_id = self.content['superevent_id']
            self.gracedb_url = self.content['urls']['gracedb']
            self.instruments = self.content['event']['instruments']
            self.group = self.content['event']['group']
            self.pipeline = self.content['event']['pipeline']
            self.far = float(self.content['event']['far'])
            self.significant = self.content['event']['significant']
            if self.group == 'CBC':
                self.classification = self.content['event']['classification']
                self.properties = self.content['event']['properties']
            else:
                self.classification = None
                self.properties = None
            self.event_time = Time(self.content['event']['time'])
            # Load the embedded skymap
            skymap_bytes = self.content['event']['skymap']
            if isinstance(skymap_bytes, str):
                # IGWN JSON skymaps are base46-encoded
                # https://emfollow.docs.ligo.org/userguide/tutorial/receiving/gcn.html
                try:
                    skymap_bytes = b64decode(skymap_bytes)
                except Exception as err:
                    raise ValueError('Failed to decode base64-encoded skymap') from err
            self.skymap = SkyMap.from_fits(skymap_bytes)
            # Get external coincidence, if any
            if self.content['external_coinc'] is not None:
                self.external = self.content['external_coinc'].copy()
                # Override the original skymap with the combined skymap
                self.skymap_original = self.skymap
                skymap_bytes = self.external['combined_skymap']
                if isinstance(skymap_bytes, str):
                    try:
                        skymap_bytes = b64decode(skymap_bytes)
                    except Exception as err:
                        raise ValueError('Failed to decode base64-encoded skymap') from err
                self.skymap = SkyMap.from_fits(skymap_bytes)
                del self.external['combined_skymap']
            else:
                self.external = None

    @classmethod
    def from_gracedb(cls, name, which_notice='last'):
        """Create a GWNotice by downloading the VOEvent XML from GraceDB."""
        # TODO: download Avro or JSON notices from GraceDB too
        if not ((isinstance(which_notice, int) and which_notice > 0) or
                which_notice in ['first', 'last']):
            raise ValueError('which_notice must be "first", "last" or a positive integer')

        template = re.compile(r'(.+)-(\d+)-(.+)')
        if template.match(name):
            # e.g. 'S230621ap-1-Preliminary'
            # Direct match for a specific notice
            event = template.match(name).groups()[0]
            url = f'https://gracedb.ligo.org/api/superevents/{event}/files/{name}.xml,0'
            if name == 'Retraction':
                return GWRetractionNotice.from_url(url)
            return cls.from_url(url)

        template = re.compile(r'(.+)-(\d+)')
        if template.match(name):
            event, number = template.match(name).groups()
            number = int(number)
        elif which_notice == 'first':
            event = name
            number = 1
        elif which_notice == 'last':
            event = name
            number = -1
        else:
            event = name
            number = int(which_notice)

        # Query the GraceDB API to get the VOEvent URL
        url = f'https://gracedb.ligo.org/api/superevents/{event}/voevents/'
        r = requests.get(url)
        data = json.loads(r.content.decode())
        if 'voevents' not in data:
            raise ValueError(f'Event {event} not found in GraceDB')
        if number == -1:
            number = len(data['voevents'])
        if number > len(data['voevents']):
            raise ValueError(f"Event {event} only has {len(data['voevents'])} notices")
        url = data['voevents'][number - 1]['links']['file']
        if 'Retraction' in url:
            return GWRetractionNotice.from_url(url)
        return cls.from_url(url)

    @property
    def strategy(self):
        """Get the observing strategy key."""
        if self.skymap is None:
            # This is very annoying, but we need to get the skymap to get the distance.
            # TODO: We could assume it is far, would that be better?
            raise ValueError('Cannot determine strategy without skymap')

        if self.external is not None:
            # External coincidences are always highest priority, regardless of other factors.
            strategy = 'GW_RANK_1'

        if self.group == 'CBC':
            # Reject events if the FAR is > 1/month, the significance cut-off for CBC events.
            # Note we explicitly look at the reported FAR here, not the significance flag
            # since it's sometimes not consistent (see S230615az).
            if (self.far * 60 * 60 * 24 * 365) > 12 and not self.significant:
                return 'IGNORE'

            # For deciding if an event is observable we use the HasRemnant property,
            # but multiply by the probability it is a BNS or NSBH to downgrade terrestrial events.
            # This is because some events can have HasRemnant=100% but still high terrestrial
            # (although the FAR cut above should have removed most of them).
            observable_metric = self.properties['HasRemnant']
            observable_metric *= (self.classification['BNS'] + self.classification['NSBH'])
            # Other factors we use:
            #  the 90% contour area (ideally the visible area, but that's tricky to calculate)
            #  the distance (the mean - 1 stddev, since the errors can be very large)
            if 'distmean' in self.skymap.header:
                distance = self.skymap.header['distmean'] - self.skymap.header['diststd']
            else:
                # Some CBC pipelines don't include distance information
                # https://git.ligo.org/emfollow/userguide/-/issues/368
                # So we'll just assume it's far
                distance = np.inf
            if observable_metric > 0.5:
                # These are the ones we always want to follow up.
                # The choice here just affects the scheduler ranking and if we send a WAKEUP alert.
                if self.skymap.get_contour_area(0.9) < 5000 and distance < 250:
                    strategy = 'GW_RANK_2'
                else:
                    strategy = 'GW_RANK_3'
            else:
                # These are most likly BBH events, which we only want to follow up if they are
                # well localised and nearby.
                if self.skymap.get_contour_area(0.9) < 5000 and distance < 250:
                    strategy = 'GW_RANK_5'
                else:
                    return 'IGNORE'

        elif self.group == 'Burst':
            # Reject events if the FAR is > 1/year, the significance cut-off for Burst events.
            if (self.far * 60 * 60 * 24 * 365) > 1 and not self.significant:
                return 'IGNORE'

            # Just like BBH events, we only want to follow up if they are well localised and nearby.
            # However Bursts don't include any distance information, so we just decide on the area.
            if self.skymap.get_contour_area(0.9) < 5000:
                strategy = 'GW_RANK_4'
            else:
                return 'IGNORE'

        else:
            raise ValueError(f'Cannot determine observing strategy for group "{self.group}"')

        # Now we alter the cadence based on the skymap area, ~how much GOTO can cover in an hour.
        # Ideally we want two epochs of each tile an hour apart. If it's going to take more than
        # an hour to cover the area, then we schedule targets so a follow-up pointing appears
        # at the same rank with a 1 hour delay to it's valid time. Once that's done the normal
        # follow-up pointing appears with a lower rank.
        # However if the skymap is small enough to cover entirely in an hour then we don't want
        # to waste time waiting for the second epoch. So just schedule all the targets to be
        # recreated immediately at a lower rank after they are observed.
        # Ideally this would only consider the visible area, but that's much more complicated!
        if self.skymap.get_contour_area(0.9) < 1000:
            return strategy + '_NARROW'
        else:
            return strategy + '_WIDE'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'
        text += f'Pipeline: {self.pipeline}\n'
        text += f'Instruments: {self.instruments}\n'
        text += f'GraceDB page: {self.gracedb_url}\n'

        # Classification info
        far_years = self.far * 60 * 60 * 24 * 365  # convert from /s to /yr
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
            far_years = self.external['time_sky_position_coincidence_far'] * 60 * 60 * 24 * 365
            if far_years > 1:
                text += f'FAR: ~{far_years:.0f} per year\n'
            else:
                text += f'FAR: ~1 per {1 / far_years:.1f} years\n'

        return text

    @property
    def short_details(self):
        """Get a short one-line summary to include when forwarding Slack messages."""
        text = f'{self.group}'
        if self.properties is not None and 'HasRemnant' in self.properties:
            text += f' (HasRemnant={self.properties["HasRemnant"]:.0%}), '
        else:
            text += ', '
        if self.skymap is not None:
            if 'distmean' in self.skymap.header:
                text += f'{self.skymap.header["distmean"]:.0f} Mpc, '
            text += f'{self.skymap.get_contour_area(0.9):.0f} sq deg, '
        far_years = self.far * 60 * 60 * 24 * 365  # convert from /s to /yr
        if far_years > 1:
            text += f'FAR: ~{far_years:.0f} per year, '
        else:
            text += f'FAR: ~1 per {1 / far_years:.1f} years, '
        text += f'strategy: `{self.strategy}`'
        if self.external is not None:
            text += '\n*External event coincidence detected!*'
        return text


class GWRetractionNotice(Notice):
    """A class to represent a Gravitational Wave retraction notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'LVC':
            raise InvalidNoticeError(f'Invalid source for GW notice: "{self.source}"')

        # Event properties
        self.event_type = 'GW'
        self.type = 'RETRACTION'
        if hasattr(self, 'top_params'):
            # Classic VOEvent format
            self.event_id = self.top_params['GraceID']['value']  # e.g. S190510g
            self.gracedb_url = self.top_params['EventPage']['value']
        else:
            # New Kafka format
            self.event_id = self.content['superevent_id']
            self.gracedb_url = self.content['urls']['gracedb']

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'RETRACTION'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'GraceDB page: {self.gracedb_url}\n'

        text += f'*THIS IS A RETRACTION OF EVENT {self.event_name}*\n'

        return text


class FermiNotice(Notice):
    """A class to represent a Fermi detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'FERMI':
            raise InvalidNoticeError(f'Invalid source for Fermi notice: "{self.source}"')
        self.source = 'Fermi'  # For nice formatting

        # Event properties
        self.event_type = 'GRB'
        if hasattr(self, 'top_params'):
            # VOEvent format, get type from the packet ID
            packet_id = int(self.top_params['Packet_Type']['value'])
            if packet_id == 115:  # FERMI_GBM_FIN_POS
                self.type = 'GBM_FIN_POS'
            else:
                msg = f'Unrecognised packet type {packet_id} for Fermi notice'
                raise InvalidNoticeError(msg)
            self.event_id = self.top_params['TrigID']['value']
            self.properties = {
                key: self.group_params['Trigger_ID'][key]['value']
                for key in self.group_params['Trigger_ID']
                if key != 'Long_short'}
            try:
                self.duration = self.group_params['Trigger_ID']['Long_short']['value']
            except KeyError:
                # Some don't have the duration
                self.duration = 'unknown'

            # Format properties
            for key in self.properties:
                if self.properties[key] == 'true':
                    self.properties[key] = True
                elif self.properties[key] == 'false':
                    self.properties[key] = False

            # Time and position
            event_location = self.message.WhereWhen['ObsDataLocation']['ObservationLocation']
            event_time = event_location['AstroCoords']['Time']['TimeInstant']['ISOTime']
            self.event_time = Time(event_time)
            event_position = event_location['AstroCoords']['Position2D']
            self.position = SkyCoord(
                ra=float(event_position['Value2']['C1']),
                dec=float(event_position['Value2']['C2']),
                unit=event_position['unit'])
            self.position_error = Angle(
                float(event_position['Error2Radius']),
                unit=event_position['unit'])
            systematic_error = Angle(5.6, unit='deg')
            self.position_error = Angle(
                np.sqrt(self.position_error ** 2 + systematic_error ** 2), unit='deg')
            # Fermi alerts don't include the URL to the HEALPix skymap,
            # because at this stage it might not have been created yet.
            # But we can try and guess it based on the typical format.
            try:
                old_url = self.top_params['LightCurve_URL']['value']
                skymap_url = old_url.replace('lc_medres34', 'healpix_all')
                self.skymap_url = skymap_url.replace('.gif', '.fit')
            except Exception:
                # Worth a try, fall back to creating our own
                self.skymap_url = None
        else:
            # For now we only process VOEvents
            raise InvalidNoticeError('Fermi notices must be VOEvents')

    @property
    def strategy(self):
        """Get the observing strategy key."""
        if self.skymap is None:
            # We need the skymap to know the area
            raise ValueError('Cannot determine strategy without skymap')

        # Select based on 1sigma area
        if self.skymap.get_contour_area(0.68) < 100:
            return 'GRB_FERMI_NARROW'
        else:
            return 'GRB_FERMI_WIDE'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Classification info
        text += f'Duration: {self.duration.capitalize()}\n'
        text += f'1σ probability area: {self.skymap.get_contour_area(0.68):.0f} sq deg\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text


class SwiftNotice(Notice):
    """A class to represent a Swift detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'SWIFT':
            raise InvalidNoticeError(f'Invalid source for Swift notice: "{self.source}"')
        self.source = 'Swift'  # For nice formatting

        # Event properties
        self.event_type = 'GRB'
        if hasattr(self, 'top_params'):
            # VOEvent format, get type from the packet ID
            packet_id = int(self.top_params['Packet_Type']['value'])
            if packet_id == 61:  # SWIFT_BAT_GRB_POS_ACK
                self.type = 'BAT_GRB_POS'
            else:
                msg = f'Unrecognised packet type {packet_id} for Swift notice'
                raise InvalidNoticeError(msg)
            self.event_id = self.top_params['TrigID']['value']
            self.properties = {
                key: self.group_params['Solution_Status'][key]['value']
                for key in self.group_params['Solution_Status']}
            # Throw out events with no star lock
            if self.properties['StarTrack_Lost_Lock'] == 'true':
                raise InvalidNoticeError('Bad Swift notice (no star lock)')

            # Format properties
            for key in self.properties:
                if self.properties[key] == 'true':
                    self.properties[key] = True
                elif self.properties[key] == 'false':
                    self.properties[key] = False

            # Time and position
            event_location = self.message.WhereWhen['ObsDataLocation']['ObservationLocation']
            event_time = event_location['AstroCoords']['Time']['TimeInstant']['ISOTime']
            self.event_time = Time(event_time)
            event_position = event_location['AstroCoords']['Position2D']
            self.position = SkyCoord(
                ra=float(event_position['Value2']['C1']),
                dec=float(event_position['Value2']['C2']),
                unit=event_position['unit'])
            self.position_error = Angle(
                float(event_position['Error2Radius']),
                unit=event_position['unit'])
            self.skymap_url = None
        else:
            # For now we only process VOEvents
            raise InvalidNoticeError('Swift notices must be VOEvents')

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'GRB_SWIFT'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text


class GECAMNotice(Notice):
    """A class to represent a GECAM detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'GECAM':
            raise InvalidNoticeError(f'Invalid source for GECAM notice: "{self.source}"')

        # Event properties
        self.event_type = 'GRB'
        if hasattr(self, 'top_params'):
            # VOEvent format, get type from the packet ID
            packet_id = int(self.top_params['Packet_Type']['value'])
            if packet_id == 189:
                self.type = 'GND'
            else:
                msg = f'Unrecognised packet type {packet_id} for GECAM notice'
                raise InvalidNoticeError(msg)
            self.event_id = self.top_params['Trigger_Number']['value']
            self.properties = {'class': self.top_params['SRC_CLASS']['value']}
            # Throw out events that aren't GRBs
            if self.properties['class'] != 'GRB':
                msg = 'GECAM notice is not a GRB ({})'.format(self.properties['class'])
                raise InvalidNoticeError(msg)

            # Format properties
            for key in self.properties:
                if self.properties[key] == 'true':
                    self.properties[key] = True
                elif self.properties[key] == 'false':
                    self.properties[key] = False

            # Time and position
            event_location = self.message.WhereWhen['ObsDataLocation']['ObservationLocation']
            event_time = event_location['AstroCoords']['Time']['TimeInstant']['ISOTime']
            self.event_time = Time(event_time)
            event_position = event_location['AstroCoords']['Position2D']
            self.position = SkyCoord(
                ra=float(event_position['Value2']['C1']),
                dec=float(event_position['Value2']['C2']),
                unit=event_position['unit'])
            self.position_error = Angle(
                float(event_position['Error2Radius']),
                unit=event_position['unit'])
            self.skymap_url = None
        else:
            # For now we only process VOEvents
            raise InvalidNoticeError('GECAM notices must be VOEvents')

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'GRB_OTHER'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text


class EinsteinProbeNotice(Notice):
    """A class to represent a Einstein Probe detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        if self.source.upper() != 'EINSTEIN_PROBE':
            raise InvalidNoticeError(f'Invalid source for EinsteinProbe notice: "{self.source}"')
        self.source = 'EinsteinProbe'  # For nice formatting

        # Event properties
        self.event_type = 'GRB'
        if '$schema' in self.content:
            # Unified GCN format
            self.type = self.content['instrument']
            if 'id' in self.content:
                self.event_id = self.content['id']
                if isinstance(self.event_id, list):
                    # Why does the schema define this as an array?
                    self.event_id = self.event_id[0]
            else:
                # Initially EP alerts had no IDs, so we just use the trigger time for the name.
                # (see the Notice.event_name property)
                self.event_id = None
            self.properties = {
                'image_energy_range': self.content['image_energy_range'],
                'net_count_rate': self.content['net_count_rate'],
                'image_snr': self.content['image_snr'],
            }

            # Time and position
            self.event_time = Time(self.content['trigger_time'])
            self.position = SkyCoord(
                ra=self.content['ra'],
                dec=self.content['dec'],
                unit='deg')
            self.position_error = Angle(
                self.content['ra_dec_error'],
                unit='deg')
            self.skymap_url = None
        else:
            # EP only produces Unified GCN format notices
            raise InvalidNoticeError('EinsteinProbe notices must be VOEvents')

    @property
    def strategy(self):
        """Get the observing strategy key."""
        return 'GRB_OTHER'

    @property
    def slack_details(self):
        """Get details for Slack messages."""
        text = f'Event: {self.event_name}\n'
        text += f'Detection time: {self.event_time.iso}\n'

        # Classification info
        text += f'SNR: {self.properties["image_snr"]:.1f}\n'

        # Position info
        text += f'Position: {self.position.to_string("hmsdms")} ({self.position.to_string()})\n'
        text += f'Position error: {self.position_error:.3f}\n'

        return text


class IceCubeNotice(Notice):
    """A class to represent an IceCube neutrino (NU) detection notice."""

    def __init__(self, payload):
        super().__init__(payload)

        # Check source
        # Note IceCube uses 'AMON' as the source for all notices
        if self.source.upper() != 'AMON':
            raise InvalidNoticeError(f'Invalid source for IceCube notice: "{self.source}"')
        self.source = 'IceCube'  # For nice formatting

        # Event properties
        self.event_type = 'NU'
        if hasattr(self, 'top_params'):
            # VOEvent format, get type from the packet ID
            packet_id = int(self.top_params['Packet_Type']['value'])
            if packet_id == 173:
                self.type = 'ASTROTRACK_GOLD'
            elif packet_id == 174:
                self.type = 'ASTROTRACK_BRONZE'
            elif packet_id == 176:
                self.type = 'CASCADE'
            else:
                msg = f'Unrecognised Neutrino packet type {packet_id} for source={self.source}'
                raise InvalidNoticeError(msg)
            self.event_id = self.top_params['AMON_ID']['value']
            self.signalness = float(self.top_params['signalness']['value'])
            self.far = float(self.top_params['FAR']['value'])

            # Time and position
            event_location = self.message.WhereWhen['ObsDataLocation']['ObservationLocation']
            event_time = event_location['AstroCoords']['Time']['TimeInstant']['ISOTime']
            self.event_time = Time(event_time)
            event_position = event_location['AstroCoords']['Position2D']
            self.position = SkyCoord(
                ra=float(event_position['Value2']['C1']),
                dec=float(event_position['Value2']['C2']),
                unit=event_position['unit'])
            self.position_error = Angle(
                float(event_position['Error2Radius']),
                unit=event_position['unit'])
            if self.type != 'CASCADE':
                # Systematic error for cascade events is 0
                systematic_error = Angle(0.2, unit='deg')
                self.position_error = Angle(
                    np.sqrt(self.position_error ** 2 + systematic_error ** 2), unit='deg')

        # Get skymap URL
        if 'skymap_fits' in self.top_params:
            self.skymap_url = self.top_params['skymap_fits']['value']
        else:
            self.skymap_url = None

    @property
    def strategy(self):
        """Get the observing strategy key."""
        if self.type == 'ASTROTRACK_GOLD':
            return 'NU_ICECUBE_GOLD'
        elif self.type == 'ASTROTRACK_BRONZE':
            return 'NU_ICECUBE_BRONZE'
        elif self.type == 'CASCADE':
            return 'NU_ICECUBE_CASCADE'
        else:
            msg = f'Cannot determine observing strategy for {self.source} {self.type} notice'
            raise ValueError(msg)

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
