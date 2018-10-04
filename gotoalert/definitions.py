#! /opt/local/bin/python3.6
"""Functions to extract event and observing data."""

import warnings

from astroplan import (AltitudeConstraint, FixedTarget, MoonSeparationConstraint,
                       Observer, is_observable)

import astropy.units as u
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.time import Time

import voeventparse as vp


def telescope(name, latitude, longitude, elevation, time_zone):
    """Create an Astroplan observer for the given telescope."""
    location = EarthLocation.from_geodetic(longitude * u.deg, latitude * u.deg, elevation * u.m)
    telescope = Observer(name=name, location=location, timezone=time_zone)
    return telescope


def goto_north():
    """Observer for GOTO-North on La Palma."""
    lapalma = EarthLocation(lon=-17.8947 * u.deg, lat=28.7636 * u.deg, height=2396 * u.m)
    telescope = Observer(name='goto_north', location=lapalma, timezone='Atlantic/Canary')
    return telescope


def goto_south():
    """Observer for a (theoretical) GOTO-South in Melbourne."""
    clayton = EarthLocation(lon=145.131389 * u.deg, lat=-37.910556 * u.deg, height=50 * u.m)
    telescope = Observer(name='goto_south', location=clayton, timezone='Australia/Melbourne')
    return telescope


ALERT_DICTIONARY = {'XRT_Pos': {'type': 'GRB',
                                'source': 'SWIFT',
                                'name': 'Swift_XRT_POS'},
                    'BAT_GRB_Pos': {'type': 'GRB',
                                    'source': 'SWIFT',
                                    'name': 'Swift_BAT_GRB_POS'},
                    'GBM_Gnd_Pos': {'type': 'GRB',
                                    'source': 'Fermi',
                                    'name': 'Fermi_GMB_GND_POS'},
                    }


def get_event_data(payload):
    """Fetch infomation about the event."""
    data = {}

    # Load the payload using voeventparse
    voevent = vp.loads(payload)

    # Get key attributes
    data['ivorn'] = voevent.attrib['ivorn']
    data['role'] = voevent.attrib['role']

    if not any([key in data['ivorn'] for key in ALERT_DICTIONARY]):
        # The event doesn't match any ones we care about
        data['type'] = None
        return data

    # If we've got here the IVORN must match one of the events we're looking for.
    # Add the known type and source to the
    for key in ALERT_DICTIONARY:
        if key in data['ivorn']:
            data.update(ALERT_DICTIONARY[key])

    # Sanity check that the sources match
    ivorn_source = data['ivorn'].split('/')[-1].split('#')[0]
    if data['source'].upper() != ivorn_source.upper():
        raise ValueError('Mismatched sources: {} and {}'.format(data['source'].upper(),
                                                                ivorn_source.upper()))

    # Get the trigger ID, if there is one
    top_params = vp.get_toplevel_params(voevent)
    if 'TrigID' in top_params:
        data['trigger_id'] = top_params['TrigID']['value']
    else:
        data['trigger_id'] = 0

    # Get contact email, if there is one
    try:
        data['contact'] = voevent.Who.Author.contactEmail
    except AttributeError:
        data['contact'] = None

    # Get event time
    data['event_time'] = Time(vp.convenience.get_event_time_as_utc(voevent, index=0))

    # Get event position (RA/DEC)
    position = vp.get_event_position(voevent)
    data['event_coord'] = SkyCoord(ra=position.ra, dec=position.dec, unit=position.units)
    data['event_coord_error'] = position.err
    data['event_target'] = FixedTarget(data['event_coord'])

    # Get event position (Galactic)
    data['object_galactic_lat'] = data['event_coord'].galactic.b
    galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
    data['dist_galactic_center'] = data['event_coord'].galactic.separation(galactic_center)

    return data


def get_obs_data(observer, target, alt_limit=30):
    """Compile infomation about the target's visibility from the given observer."""
    # Get midnight and astronomicla twilight times
    current_time = Time.now()
    midnight = observer.midnight(current_time, which='next')
    sun_set = observer.twilight_evening_astronomical(midnight, which='previous')
    sun_rise = observer.twilight_morning_astronomical(midnight, which='next')

    time_range = Time([sun_set, sun_rise])

    # Apply a constraint on altitude
    min_alt = alt_limit * u.deg
    alt_constraint = AltitudeConstraint(min=min_alt, max=None)
    alt_observable = is_observable(alt_constraint, observer, target, time_range=time_range)[0]

    # Get target rise and set times
    if alt_observable:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            target_rise = observer.target_rise_time(midnight, target,
                                                    which='nearest', horizon=min_alt)
            target_set = observer.target_set_time(target_rise, target,
                                                  which='next', horizon=min_alt)

        # Get observation times
        observation_start = target_rise
        observation_end = target_set
        if target_rise.jd < 0 or target_set.jd < 0:
            # target is always above the horizon, so visible all night
            observation_start = sun_set
            observation_end = sun_rise
        if target_rise < sun_set:
            # target is already up when the sun sets
            observation_start = sun_set
        if target_set > sun_rise:
            # target sets after the sun rises
            observation_end = sun_rise

    else:
        target_rise = None
        target_set = None
        observation_start = None
        observation_end = None

    # Apply a constraint on distance from the Moon
    min_moon = 5 * u.deg
    moon_constraint = MoonSeparationConstraint(min=min_moon, max=None)
    moon_observable = is_observable(moon_constraint, observer, target, time_range=time_range)[0]

    data = {'observer': observer,
            'current_time': current_time,
            'midnight': midnight,
            'sun_set': sun_set,
            'sun_rise': sun_rise,
            'target_rise': target_rise,
            'target_set': target_set,
            'observation_start': observation_start,
            'observation_end': observation_end,
            'alt_constraint': alt_constraint,
            'alt_observable': alt_observable,
            'moon_constraint': moon_constraint,
            'moon_observable': moon_observable,
            }

    return data
