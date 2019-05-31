#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import logging

import astropy.units as u

from . import database as db
from . import params
from .definitions import get_obs_data, goto_north, goto_south
from .events import Event
from .output import create_webpages
from .slack import send_event_message
from .strategy import get_event_strategy


def event_handler(event, write_html=False, send_messages=False, log=None):
    """Handle a new Event.

    Returns the Event if it is interesting, or None if it's been rejected.

    Parameters
    ----------
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

    # Log IVORN
    log.info('Handling Event {}'.format(event.ivorn))

    # Check if it's an event we want to process, otherwise return None
    if event.role in params.IGNORE_ROLES:
        log.warning('Ignoring {} event'.format(event.role))
        return None
    elif not event.interesting:
        log.warning('Ignoring uninteresting event')
        return None

    # It passed the checks: it's an interesting event!
    log.info('Processing interesting {} Event {}'.format(event.type, event.name))

    # Fetch the event skymap
    if hasattr(event, 'get_skymap'):
        # Not all "interesting" events will have a skymap (e.g. retractions)
        log.debug('Fetching event skymap')
        event.get_skymap()
        log.debug('Skymap created')

    # Send a Slack report
    if send_messages:
        log.debug('Sending Slack report')
        send_event_message(event)
        log.debug('Slack report sent')

    # Get the observing strategy for this event
    strategy_dict = get_event_strategy(event)

    # Add the event into the GOTO observation DB
    log.info('Inserting event {} into GOTO database'.format(event.name))
    try:
        # First we need to see if there's a previous instance of the same event already in the db
        # If so, then delete any still pending pointings and mpointings assosiated with the event
        log.debug('Checking for previous events in database')
        db.remove_previous_events(event, log)

        # Then add the new pointings
        log.debug('Adding to database')
        db.add_to_database(event, strategy_dict, log)
        log.info('Database insersion complete')

    except Exception:
        log.warning('Unable to insert event into database')
        raise

    # Get observing data for the event at each site
    observers = [goto_north(), goto_south()]
    obs_data = get_obs_data(event.target, observers, event.creation_time)

    # Parse the event for each site
    for site_name in obs_data:
        site_data = obs_data[site_name]

        # Check if it's observable
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

    log.info('Event {} processed'.format(event.name))
    return event


def payload_handler(payload, log=None, write_html=True, send_messages=False):
    """Handle a VOEvent payload.

    Returns the Event if it is interesting, or None if it's been rejected.
    """
    # Create event from the payload
    event = Event.from_payload(payload)

    # Run the event handler
    event_handler(event, log, write_html, send_messages)
