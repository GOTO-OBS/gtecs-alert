#! /opt/local/bin/python3.6

from astroplan import (AltitudeConstraint, FixedTarget, MoonSeparationConstraint,
                       Observer, is_observable)

import astropy.units as u
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.time import Time

import numpy as np

from pytz import timezone

import voeventparse as vp


def telescope(name, latitude, longitude, elevation, time_zone):
    """Create an Astroplan observer for the given site."""
    location = EarthLocation.from_geodetic(longitude, latitude, elevation * u.m)
    telescope = Observer(name=name, location=location, timezone=timezone(time_zone))
    return telescope


def event_definitions(v, current_time):

    # Get IVORN
    ivorn = v.attrib['ivorn']

    # Get event time
    event_time = vp.convenience.get_event_time_as_utc(v, index=0)

    # Get event position (RA/DEC)
    pos = vp.get_event_position(v)
    event_coord = SkyCoord(ra=pos.ra, dec=pos.dec, unit=pos.units)
    coorderr = pos.err
    event_str = str(event_coord.ra.value) + ' ' + str(event_coord.dec.value)
    event_target = FixedTarget.from_name(event_str)

    # Get event position (Galactic)
    coord_deg = SkyCoord(event_coord, unit='deg')
    object_galactic_pos = coord_deg.galactic
    object_galactic_lat = coord_deg.galactic.b
    galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
    dist_galactic_center = object_galactic_pos.separation(galactic_center)

    data = {'current_time': current_time,
            'event_coord': event_coord,
            'event_coord_error': coorderr,
            'event_target': event_target,
            'event_time': event_time,
            'ivorn': ivorn,
            'object_galactic_lat': object_galactic_lat,
            'dist_galactic_center': dist_galactic_center,
            }
    return data


def observing_definitions(telescope, midnight_time, alt_limit, event_dictionary):

    # Get current date
    current_time = Time(event_dictionary['current_time'])
    currenttime_string = str(current_time)
    current_date = currenttime_string[:10]

    # Get midnight time
    midnight_time = Time(['2018-01-1 ' + midnight_time])
    midnight_time_str = str(midnight_time)[12:24]
    midnight = current_date + midnight_time_str
    midnight_iso = Time(midnight, format='iso', scale='utc')
    midnight_jd = Time(midnight_iso.jd, format='jd', scale='utc')

    # Get sunrise, sunset and twilight times
    sun_set_tonight = telescope.sun_set_time(midnight_jd, which='previous')
    sun_rise_tonight = telescope.sun_rise_time(midnight_jd, which='next')
    dark_sunset_tonight = telescope.twilight_evening_astronomical(midnight_jd, which='previous')
    dark_sunrise_tonight = telescope.twilight_morning_astronomical(midnight_jd, which='next')

    # Get target rise and set times
    event_target = event_dictionary['event_target']
    target_rise = telescope.target_rise_time(dark_sunset_tonight, event_target, which='nearest')
    target_set = telescope.target_set_time(dark_sunrise_tonight, event_target, which='nearest')
    if target_set.jd < 0 and target_rise.jd < 0:
        target_rise = Time.now() - 0.9 * u.day
        target_set = Time.now() + 1 * u.day

    # Find the earilest and latest possible start times for observing
    observation_start = np.max([target_rise, dark_sunset_tonight])
    observation_start_iso = observation_start.iso
    observation_end = np.min([target_set, dark_sunrise_tonight])
    observation_end_iso = observation_end.iso

    # Create an array of times during the night
    nighttime = dark_sunset_tonight + np.arange(0, 4, 0.25) * u.hour

    # Create an array of times while the target is observable
    delta_t = observation_end - observation_start
    airmass_time = observation_start + delta_t * np.linspace(0, 1, 75)

    # Create a constraint on altitude
    min_alt = alt_limit * u.deg
    max_alt = 90 * u.deg
    alt_constraint = [AltitudeConstraint(min_alt, max_alt)]

    # Create a constraint on distance from the Moon
    min_moon = 5 * u.deg
    max_moon = None
    moon_constraint = [MoonSeparationConstraint(min=min_moon, max=max_moon)]

    # Apply constraints to the target during the whole night
    time_range = Time([dark_sunset_tonight, dark_sunrise_tonight])
    alt_observable = is_observable(alt_constraint, telescope, event_target,
                                   time_range=time_range)[0]
    moon_observable = is_observable(moon_constraint, telescope, event_target,
                                    time_range=time_range)[0]

    # Apply altitude constraint only during the observable period
    time_range_obs = Time([observation_start_iso, observation_end_iso])
    final_constraint = is_observable(alt_constraint, telescope, event_target,
                                     time_range=time_range_obs)[0]

    data = {'midnight_iso': midnight_iso,
            'midnight_jd': midnight_jd,
            'sun_set_tonight': sun_set_tonight,
            'sun_rise_tonight': sun_rise_tonight,
            'dark_sunset_tonight': dark_sunset_tonight,
            'dark_sunrise_tonight': dark_sunrise_tonight,
            'target_rise': target_rise,
            'target_set': target_set,
            'observation_start': observation_start,
            'observation_start_iso': observation_start_iso,
            'observation_end': observation_end,
            'observation_end_iso': observation_end_iso,
            'night_time': nighttime,
            'delta_t': delta_t,
            'airmass_time': airmass_time,
            'alt_constraint': alt_constraint,
            'moon_constraint': moon_constraint,
            'time_range': time_range,
            'alt_observable': alt_observable,
            'moon_observable': moon_observable,
            'time_range_obs': time_range_obs,
            'final_constraint': final_constraint,
            }

    return data
