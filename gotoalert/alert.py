#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import logging

from . import database as db
from . import params
from .events import Event
from .slack import send_event_message
from .strategy import get_event_strategy


def event_handler(event, send_messages=False, log=None):
    """Handle a new Event.

    Returns the Event if it is interesting, or None if it's been rejected.

    Parameters
    ----------
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

    log.info('Event {} processed'.format(event.name))
    return event


def payload_handler(payload, send_messages=False):
    """Handle a VOEvent payload.

    Returns the Event if it is interesting, or None if it's been rejected.
    """
    # Create event from the payload
    event = Event.from_payload(payload)

    # Run the event handler
    event_handler(event, send_messages)
