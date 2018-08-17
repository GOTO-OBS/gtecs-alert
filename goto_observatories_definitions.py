#! /opt/local/bin/python3.6

from astroplan import Observer, FixedTarget, is_observable, AltitudeConstraint, AirmassConstraint, AtNightConstraint, MoonSeparationConstraint
from astropy.coordinates import EarthLocation, SkyCoord
import astropy.units as u
from pytz import timezone
import voeventparse as vp
from voeventparse import get_toplevel_params, get_event_time_as_utc
from astropy.time import Time
import numpy as np
from astropy import units

currenttime = Time.now().utc
current_time = Time(currenttime)
currenttime_string = str(currenttime)
current_date = currenttime_string[:10]


def telescope(name2, latitude, longitude, elevation, time_zone):
    telescope = Observer(name=name2,location=EarthLocation.from_geodetic(longitude, latitude, elevation * u.m),
                    timezone=timezone(time_zone))
    return telescope

def params(file):
    params = vp.get_grouped_params(file)
    return params

def top_params(file):
    top_params = get_toplevel_params(file)
    return top_params


def event_definitions(file):
    c = vp.get_event_position(file)

    eventtime = vp.convenience.get_event_time_as_utc(file, index=0)
    coord = SkyCoord(ra=c.ra, dec=c.dec, unit=c.units)
    coorderr = c.err
    eventcoord = FixedTarget.from_name(str(coord.ra.value)+' '+str(coord.dec.value))
    ivorn = file.attrib['ivorn']
    coorddeg = SkyCoord(coord, unit='deg')
    object_galactic_pos = coorddeg.galactic
    object_galactic_lat = coorddeg.galactic.b
    galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
    dist_galactic_center = object_galactic_pos.separation(galactic_center)

    Data = {
    "current_time": currenttime,
    "ra_dec": coord,
    "ra_dec_error": coorderr,
    "ra_dec_formatted": eventcoord,
    "event_time": eventtime,
    "ivorn": ivorn,
    "object_galactic_lat": object_galactic_lat,
    "dist_galactic_center": dist_galactic_center
    }
    return Data


def observing_definitions(time, length, alt, telescope, ra_dec_formatted):
    midnight_time = Time(['2018-01-1 '+time])
    midnight_time_str = str(midnight_time)[12:24]
    midnightcombine1 = current_date + midnight_time_str
    midnight_iso = Time(midnightcombine1, format='iso', scale='utc')
    midnight_jd = Time(midnight_iso.jd, format='jd', scale='utc')

    sun_set_tonight = telescope.sun_set_time(midnight_jd, which='previous')
    sun_rise_tonight = telescope.sun_rise_time(midnight_jd, which='next')
    dark_sunset_tonight = telescope.twilight_evening_astronomical(midnight_jd, which='previous')
    dark_sunrise_tonight = telescope.twilight_morning_astronomical(midnight_jd, which='next')
    target_rise = telescope.target_rise_time(dark_sunset_tonight, ra_dec_formatted, which='nearest')
    target_set = telescope.target_set_time(dark_sunrise_tonight, ra_dec_formatted, which='nearest')

    if target_set.jd < 0 and target_rise_south.jd < 0:
        target_rise = Time.now() - 0.9 * u.day
        target_set = Time.now() + 1 * u.day

    observation_start = np.max([target_rise, dark_sunset_tonight])
    observation_start_iso = observation_start.iso
    observation_end = np.min([target_set, dark_sunrise_tonight])
    observation_end_iso = observation_end.iso
    nighttime = dark_sunset_tonight + np.arange(0, length, 0.25) * units.hour
    delta_t = observation_end - observation_start
    airmass_time = observation_start +delta_t*np.linspace(0, 1, 75)

    alt_constraint = [AltitudeConstraint(alt*u.deg, 90*u.deg)]
    moon_constraint = [MoonSeparationConstraint(min=5*u.deg, max=None)]

    time_range = Time([dark_sunset_tonight, dark_sunrise_tonight])
    alt_observable = is_observable(alt_constraint, telescope, ra_dec_formatted, time_range=time_range)
    moon_observable = is_observable(moon_constraint, telescope, ra_dec_formatted, time_range=time_range)
    time_range_obs = Time([observation_start_iso, observation_end_iso])
    final_constraint = is_observable(alt_constraint, telescope, ra_dec_formatted, time_range=time_range_obs)

    if str(alt_observable) == "[ True]":
        alt_observable_adjusted = str(alt_observable)[2:6]

    else:
        alt_observable_adjusted = str(alt_observable)[1:6]

    if str(moon_observable) == "[ True]":
        moon_observable_adjusted = str(moon_observable)[2:6]

    else:
        moon_observable_adjusted = str(moon_observable)[1:6]

    Data = {
    "midnight_iso": midnight_iso,
    "midnight_jd": midnight_jd,
    "sun_set_tonight": sun_set_tonight,
    "sun_rise_tonight": sun_rise_tonight,
    "dark_sunset_tonight": dark_sunset_tonight,
    "dark_sunrise_tonight": dark_sunrise_tonight,
    "target_rise": target_rise,
    "target_set": target_set,
    "observation_start": observation_start,
    "observation_start_iso": observation_start_iso,
    "observation_end": observation_end,
    "observation_end_iso": observation_end_iso,
    "night_time": nighttime,
    "delta_t": delta_t,
    "airmass_time": airmass_time,
    "alt_constraint": alt_constraint,
    "moon_constraint": moon_constraint,
    "time_range": time_range,
    "alt_observable": alt_observable,
    "moon_observable": moon_observable,
    "alt_observable_adjusted": alt_observable_adjusted,
    "moon_observable_adjusted": moon_observable_adjusted,
    "time_range_obs": time_range_obs,
    "final_constraint": final_constraint
    }

    return Data
