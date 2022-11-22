"""Event handlers for VOEvents."""

import logging

from . import database as db
from .slack import send_database_report, send_event_report, send_slack_msg, send_strategy_report


def event_handler(event, send_messages=False, log=None):
    """Handle a new Event.

    Parameters
    ----------
    event : `gototile.events.Event`
        The Event to handle

    send_messages : bool, optional
        If True, send Slack messages.
        Default is False.
    log : logging.Logger, optional
        If given, direct log messages to this logger.
        If None, a new logger is created.

    Returns
    -------
    processed : bool
        Will return True if the Event was processed (if event.interesting == True).
        If the Event was not interesting then it will be ignored and return False.

    """
    # Create a logger if one isn't given
    if log is None:
        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('event_handler')
        log.setLevel(level=logging.DEBUG)
    log.info('Handling Event {}'.format(event.ivorn))

    # 1) Check if it's an event we want to process, otherwise return here
    if not event.interesting:
        log.warning('Ignoring uninteresting event (type={}, role={})'.format(event.type,
                                                                             event.role))
        return False
    log.info('Processing interesting {} Event {}'.format(event.type, event.name))

    # Send initial Slack report
    if send_messages:
        log.debug('Sending initial Slack report')
        msg = '*Processing new {} {} event: {}*'.format(event.source, event.type, event.id)
        send_slack_msg(msg)
        log.debug('Slack report sent')

    # 2) Fetch the event skymap
    log.debug('Fetching event skymap')
    event.get_skymap()
    log.debug('Skymap created')

    # Send Slack event report
    if send_messages:
        log.debug('Sending Slack event report')
        try:
            send_event_report(event)
            log.debug('Slack report sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # 3) Get the observing strategy for this event (stored on the event as event.strategy)
    #    NB we can only do this after getting the skymap, because GW events need the distance.
    log.debug('Fetching event strategy')
    event.get_strategy()
    log.debug('Using strategy {}'.format(event.strategy['strategy']))

    # Send Slack strategy report
    if send_messages:
        log.debug('Sending Slack strategy report')
        try:
            send_strategy_report(event)
            log.debug('Slack report sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # 4) Add the event into the GOTO observation DB
    log.info('Inserting event {} into GOTO database'.format(event.name))
    try:
        # First we need to see if there's a previous instance of the same event already in the db
        # If so, then delete any still pending pointings and mpointings associated with the event
        log.debug('Checking for previous events in database')
        db.remove_previous_events(event, log)

        # Then add the new pointings
        log.debug('Adding to database')
        db.add_to_database(event, log)
        log.info('Database insersion complete')

        # Send Slack database report
        if send_messages:
            log.debug('Sending Slack database report')
            try:
                send_database_report(event)
                log.debug('Slack report sent')
            except Exception as err:
                log.error('Error sending Slack report')
                log.debug(err.__class__.__name__, exc_info=True)

    except Exception as err:
        log.error('Unable to insert event into database')
        log.debug(err.__class__.__name__, exc_info=True)

        # Send Slack error report
        if send_messages:
            log.debug('Sending Slack error report')
            msg = '*ERROR*: Failed to insert event {} into database'.format(event.name)
            send_slack_msg(msg)
            log.debug('Slack report sent')

        raise

    # 5) Done
    log.info('Event {} processed'.format(event.name))
    return True
