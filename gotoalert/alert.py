#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import os

import astropy.units as u

from . import coms
from .csv2htmltable import write_table
from .definitions import get_event_data, get_obs_data, goto_north, goto_south
from .slack_message import slackmessage

path = "./www"
send_messages = False


def check_event_type(event_data):
    """Check if the event is something we want to process."""
    # Check role
    print('Event is marked as "{}"'.format(event_data['role']))
    if event_data['role'] in ['test', 'utility']:
        raise ValueError('Ignoring {} event'.format(event_data['role']))

    # Get alert name
    if event_data['type'] is None:
        raise ValueError('Ignoring unrecognised event type: {}'.format(event_data['ivorn']))
    print('Recognised event type: {} ({})'.format(event_data['name'], event_data['type']))


def check_event_position(event_data):
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


def check_obs_params(obs_data):
    """Check if the event is observable from a paticular telescope."""
    telescope = obs_data['observer']
    name = telescope.name

    # Check if the target rises above the horizon
    if not obs_data['alt_observable']:
        raise ValueError('Target does not rise above minimum altitude at {}'.format(name))

    # Check if the target is visible for enough time
    if (obs_data['observation_end'] - obs_data['observation_start']) < 1.5 * u.hour:
        raise ValueError('Target is not up longer then 1:30 at {} during the night'.format(name))


def parse(event_data, all_obs_data, telescope):
    """Parse an event for a given telescope."""
    name = event_data['name']
    trigger_id = event_data['trigger_id']

    obs_data = all_obs_data[telescope.name]

    # Find file paths
    file_name = name + trigger_id
    file_path = "./www/{}_transients/".format(telescope.name)

    # Create graphs
    coms.create_graphs(file_path, event_data, obs_data)

    # Write HTML
    coms.write_html(file_path, event_data, obs_data)

    # Send email if enabled
    email_subject = "Detection from {}".format(telescope.name)
    email_body = "{} Detection: See more at http://118.138.235.166/~obrads".format(name)
    if send_messages:
        coms.send_email(fromaddr="lapalmaobservatory@gmail.com",
                        toaddr="aobr10@student.monash.edu",
                        subject=email_subject,
                        body=email_body,
                        password="lapalmaobservatory1",
                        file_path=file_path,
                        file_name=file_name)

    # Write CSV
    csv_file = telescope.name + ".csv"
    coms.write_csv(os.path.join(file_path, csv_file), event_data, all_obs_data)

    # Write latest 10 page
    topten_file = "recent_ten.html"
    coms.write_topten(file_path, csv_file, topten_file)

    # Send message to Slack
    if telescope.name == "goto_north" and send_messages:
        slackmessage(name,
                     str(event_data["event_time"])[:22],
                     str(event_data["event_coord"].ra.deg),
                     str(event_data["event_coord"].dec.deg),
                     file_name)
        print("sent message to slack")

    # Convert CSVs to HTML
    write_table(file_path, csv_file)


def event_handler(v):
    """Handle a VOEvent payload."""
    # Get event data from the payload
    event_data = get_event_data(v)

    # Check if it's an event we want to process
    try:
        check_event_type(event_data)
        check_event_position(event_data)
    except Exception as err:
        print(err)
        return

    # Get observing data for the event with each telescope
    target = event_data['event_target']
    telescopes = [goto_north(), goto_south()]
    all_obs_data = {}
    for telescope in telescopes:
        all_obs_data[telescope.name] = get_obs_data(telescope, target)

    # write master csv file
    coms.write_csv(os.path.join(path, "master.csv"), event_data, all_obs_data)

    # Parse the event for each site
    for telescope in telescopes:
        obs_data = all_obs_data[telescope.name]

        # Check if it's observable
        try:
            check_obs_params(obs_data)
        except Exception as err:
            print(err)
            continue

        # Parse the event
        parse(event_data, all_obs_data, telescope)

    print("done")
