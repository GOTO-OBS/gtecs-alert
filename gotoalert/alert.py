#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import logging

import astropy.units as u

from .comms import send_email, send_slackmessage
from .definitions import get_event_data, get_obs_data, goto_north, goto_south
from .output import create_webpages

PATH = "./www"


def check_event_type(event_data, log):
    """Check if the event is something we want to process."""
    # Check role
    log.info('Event is marked as "{}"'.format(event_data['role']))
    if event_data['role'] in ['test', 'utility']:
        raise ValueError('Ignoring {} event'.format(event_data['role']))

    # Get alert name
    if event_data['type'] is None:
        raise ValueError('Ignoring unrecognised event type: {}'.format(event_data['ivorn']))
    log.info('Recognised event type: {} ({})'.format(event_data['name'], event_data['type']))


def check_event_position(event_data, log):
    """Check if the event position is too close to the galaxy ."""
    # Check galactic latitude
    if -8 < event_data['object_galactic_lat'].value < 8:
        raise ValueError('Event too close to the Galactic plane (Lat {})'.format(
                         event_data['object_galactic_lat'].value
                         ))

    # Check distance from galactic center
    if event_data['dist_galactic_center'].value < 15:
        raise ValueError(' Event too close to the Galactic centre (Dist {})'.format(
                         event_data['dist_galactic_center'].value
                         ))

    log.info('Event sufficiently far away from the galactic plane')


def check_obs_params(obs_data, log):
    """Check if the event is observable from a paticular telescope."""
    telescope = obs_data['observer']
    name = telescope.name

    # Check if the target rises above the horizon
    if not obs_data['alt_observable']:
        raise ValueError('Target does not rise above minimum altitude at {}'.format(name))
    log.info('Target is visible tonight at {}'.format(name))

    # Check if the target is visible for enough time
    if (obs_data['observation_end'] - obs_data['observation_start']) < 1.5 * u.hour:
        raise ValueError('Target is not up longer then 1:30 at {} during the night'.format(name))
    log.info('Target is up for longer than 1:30 tonight at {}'.format(name))


def event_handler(payload, log=None, write_html=True, send_messages=False):
    """Handle a VOEvent payload."""
    # Create a logger if one isn't given
    if log is None:
        logging.basicConfig(level=logging.DEBUG)
        log = logging.getLogger('goto-alert')

    # Get event data from the payload
    event_data = get_event_data(payload)

    # Check if it's an event we want to process
    try:
        check_event_type(event_data, log)
        check_event_position(event_data, log)
    except Exception as err:
        log.warning(err)
        return

    # Get observing data for the event with each telescope
    target = event_data['event_target']
    telescopes = [goto_north(), goto_south()]
    all_obs_data = {}
    for telescope in telescopes:
        all_obs_data[telescope.name] = get_obs_data(telescope, target)

    # Parse the event for each site
    for telescope in telescopes:
        obs_data = all_obs_data[telescope.name]

        # Check if it's observable
        try:
            check_obs_params(obs_data, log)
        except Exception as err:
            log.warning(err)
            continue

        # Create and update web pages
        if write_html:
            create_webpages(event_data, all_obs_data, telescope, web_path=PATH)
            log.debug('HTML page written for {}'.format(telescope.name))

        # Send messages
        if send_messages:
            event_name = event_data['name']
            trigger_id = event_data['trigger_id']
            file_name = event_name + trigger_id
            file_path = PATH + "{}_transients/".format(telescope.name)

            # Send email
            email_subject = "Detection from {}".format(telescope.name)
            email_link = 'http://118.138.235.166/~obrads'
            email_body = "{} Detection: See more at {}".format(event_name, email_link)

            send_email(fromaddr="lapalmaobservatory@gmail.com",
                       toaddr="aobr10@student.monash.edu",
                       subject=email_subject,
                       body=email_body,
                       password="lapalmaobservatory1",
                       file_path=file_path,
                       file_name=file_name)
            log.debug('Sent email alert for {}'.format(telescope.name))

            # Send message to Slack
            if telescope.name == "goto_north":
                send_slackmessage(event_name,
                                  str(event_data["event_time"])[:22],
                                  str(event_data["event_coord"].ra.deg),
                                  str(event_data["event_coord"].dec.deg),
                                  file_name)
                log.debug('Sent slack message for {}'.format(telescope.name))

    log.info('Event {}{} processed'.format(event_data['name'], event_data['trigger_id']))
