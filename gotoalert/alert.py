#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import logging

import astropy.units as u

from . import params
from .comms import send_email, send_slackmessage
from .database import db_insert
from .definitions import get_obs_data, goto_north, goto_south
from .events import Event
from .output import create_webpages


def check_event_type(event, log):
    """Check if the event is something we want to process."""
    # Check role
    log.info('Event is marked as "{}"'.format(event.role))
    if event.role in params.IGNORE_ROLES:
        raise ValueError('Ignoring {} event'.format(event.role))

    # Get alert name
    if event.type is 'Unknown':
        raise ValueError('Ignoring unrecognised event type: {}'.format(event.ivorn))
    log.info('Recognised event type: {} ({})'.format(event.notice, event.type))


def check_event_position(event, log):
    """Check if the event position is too close to the galaxy ."""
    # Check galactic latitude
    if event.gal_lat and abs(event.gal_lat) < params.MIN_GALACTIC_LATITUDE:
        raise ValueError('Event too close to the galactic plane (Lat {:.2f})'.format(event.gal_lat))

    # Check distance from galactic center
    if event.gal_dist and event.gal_dist < params.MIN_GALACTIC_DISTANCE:
        raise ValueError('Event too close to the galactic centre (Dist {})'.format(event.gal_dist))

    log.info('Event sufficiently far away from the galactic plane')


def check_obs_params(site_data, log):
    """Check if the event is observable from a paticular site."""
    observer = site_data['observer']
    name = observer.name

    # Check if the target rises above the horizon
    if not site_data['alt_observable']:
        raise ValueError('Target does not rise above minimum altitude at {}'.format(name))
    log.info('Target is visible tonight at {}'.format(name))

    # Check if the target is visible for enough time
    if (site_data['observation_end'] - site_data['observation_start']) < 1.5 * u.hour:
        raise ValueError('Target is not up longer then 1:30 at {} during the night'.format(name))
    log.info('Target is up for longer than 1:30 tonight at {}'.format(name))


def event_handler(event, log=None, write_html=True, send_messages=False):
    """Handle a new Event.

    Returns the Event if it is interesting, or None if it's been rejected.
    """
    # Create a logger if one isn't given
    if log is None:
        logging.basicConfig(level=logging.DEBUG)
        log = logging.getLogger('goto-alert')

    # Check if it's an event we want to process
    try:
        check_event_type(event, log)
    except Exception as err:
        log.warning(err)
        return None

    # Check if it's too close to the galaxy
    try:
        if params.MIN_GALACTIC_LATITUDE or params.MIN_GALACTIC_DISTANCE:
            check_event_position(event, log)
    except Exception as err:
        log.warning(err)
        return None

    # It's an interesting event!

    # Add the event into the GOTO observation DB
    db_insert(event, log, on_grid=True)

    # Get observing data for the event at each site
    observers = [goto_north(), goto_south()]
    obs_data = get_obs_data(event.target, observers, event.creation_time)

    # Parse the event for each site
    for site_name in obs_data:
        site_data = obs_data[site_name]

        # Check if it's observable
        try:
            check_obs_params(site_data, log)
        except Exception as err:
            log.warning(err)
            continue

        # Create and update web pages
        if write_html:
            create_webpages(event, obs_data, site_name, web_path=params.HTML_PATH)
            log.debug('HTML page written for {}'.format(site_name))

        # Send messages
        if send_messages:
            file_name = event.name
            file_path = params.HTML_PATH + "{}_transients/".format(site_name)

            # Send email
            email_subject = "Detection from {}".format(site_name)
            email_link = 'http://118.138.235.166/~obrads'
            email_body = "{} Detection: See more at {}".format(event.type, email_link)

            send_email(fromaddr="lapalmaobservatory@gmail.com",
                       toaddr="aobr10@student.monash.edu",
                       subject=email_subject,
                       body=email_body,
                       password="lapalmaobservatory1",
                       file_path=file_path,
                       file_name=file_name)
            log.debug('Sent email alert for {}'.format(site_name))

            # Send message to Slack
            if site_name == "goto_north":
                send_slackmessage(event, file_name)
                log.debug('Sent slack message for {}'.format(site_name))

    log.info('Event {} processed'.format(event.name))
    return event


def payload_handler(payload, log=None, write_html=True, send_messages=False):
    """Handle a VOEvent payload.

    Returns the Event if it is interesting, or None if it's been rejected.
    """
    # Create event from the payload
    event = Event(payload)

    # Run the event handler
    event_handler(event, log, write_html, send_messages)
