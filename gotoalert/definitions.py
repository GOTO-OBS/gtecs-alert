#! /opt/local/bin/python3.6
"""Functions to extract event and observing data."""

import warnings

from astroplan import (AltitudeConstraint, MoonSeparationConstraint, Observer, is_observable)

import astropy.units as u
from astropy.coordinates import EarthLocation
from astropy.time import Time


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


def get_obs_data(target, observers, current_time, alt_limit=30):
    """Compile infomation about the target's visibility from the given observers."""
    all_data = {}
    if target is None:
        return all_data

    for observer in observers:
        data = {}
        data['observer'] = observer
        data['current_time'] = current_time

        # Get midnight and astronomical twilight times
        midnight = observer.midnight(current_time, which='next')
        sun_set = observer.twilight_evening_astronomical(midnight, which='previous')
        sun_rise = observer.twilight_morning_astronomical(midnight, which='next')
        dark_time = Time([sun_set, sun_rise])
        data['midnight'] = midnight
        data['sun_set'] = sun_set
        data['sun_rise'] = sun_rise

        # Apply a constraint on altitude
        min_alt = alt_limit * u.deg
        alt_constraint = AltitudeConstraint(min=min_alt, max=None)
        alt_observable = is_observable(alt_constraint, observer, target, time_range=dark_time)[0]
        data['alt_constraint'] = alt_constraint
        data['alt_observable'] = alt_observable

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

            data['target_rise'] = target_rise
            data['target_set'] = target_set
            data['observation_start'] = observation_start
            data['observation_end'] = observation_end
        else:
            data['target_rise'] = None
            data['target_set'] = None
            data['observation_start'] = None
            data['observation_end'] = None

        # Apply a constraint on distance from the Moon
        min_moon = 5 * u.deg
        moon_constraint = MoonSeparationConstraint(min=min_moon, max=None)
        moon_observable = is_observable(moon_constraint, observer, target, time_range=dark_time)[0]
        data['moon_constraint'] = moon_constraint
        data['moon_observable'] = moon_observable

        all_data[observer.name] = data

    return all_data
