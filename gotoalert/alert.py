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


def event_handler(event, force_process=False, write_html=False, send_messages=False, log=None):
    """Handle a new Event.

    Returns the Event if it is interesting, or None if it's been rejected.

    Parameters
    ----------
    force_process : bool, optional
        If True, ignore the event filters (e.g. event roles, obs parameters) and process anyway.
        Note if the Packet_Type is not in events.EVENT_DICTONARY then it still can't be
        processed. This is more for testing events that we would usually process, not for
        handling completely new events.
        Default is False.

    write_html : bool, optional
        If True, write out HTML web pages to params.HTML_PATH.
        Default is False.

    send_messages : bool, optional
        If True, send Slack messages.
        Default is False.

    """
    # Create a logger if one isn't given
    if log is None:
        logging.basicConfig(level=logging.DEBUG)
        log = logging.getLogger('goto-alert')

    # Check if it's an event we want to process
    if not force_process:
        # Check role
        log.info('Event is marked as "{}"'.format(event.role))
        if event.role in params.IGNORE_ROLES:
            log.warning('Ignoring {} event'.format(event.role))
            return None

        # Check type
        if event.type is 'Unknown':
            log.warning('Ignoring unrecognised event type: {}'.format(event.ivorn))
            return None
        log.info('Recognised event type: {} ({})'.format(event.notice, event.type))

        # Check galactic latitude
        if (params.MIN_GALACTIC_LATITUDE and event.gal_lat and
                abs(event.gal_lat) < params.MIN_GALACTIC_LATITUDE):
            log.warning('Event too close to the galactic plane (Lat {:.2f})'.format(event.gal_lat))
            return None

        # Check distance from galactic center
        if (params.MIN_GALACTIC_DISTANCE and event.gal_dist and
                event.gal_dist < params.MIN_GALACTIC_DISTANCE):
            log.warning('Event too close to the galactic centre (Dist {})'.format(event.gal_dist))
            return None

    # It passed the checks: it's an interesting event!

    # Add the event into the GOTO observation DB
    db_insert(event, log, on_grid=params.ON_GRID)

    # Get observing data for the event at each site
    observers = [goto_north(), goto_south()]
    obs_data = get_obs_data(event.target, observers, event.creation_time)

    # Parse the event for each site
    for site_name in obs_data:
        site_data = obs_data[site_name]

        # Check if it's observable
        if not force_process:
            # Check if the target rises above the horizon
            if not site_data['alt_observable']:
                log.warning('Target does not rise above minimum altitude at {}'.format(site_name))
                continue
            log.info('Target is visible tonight at {}'.format(site_name))

            # Check if the target is visible for enough time
            if (site_data['observation_end'] - site_data['observation_start']) < 1.5 * u.hour:
                log.warning('Target is not up longer then 1:30 at {} tonight'.format(site_name))
                continue
            log.info('Target is up for longer than 1:30 tonight at {}'.format(site_name))

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
