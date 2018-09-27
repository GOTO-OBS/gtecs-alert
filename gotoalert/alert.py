#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import os

import astropy.units as u

from . import coms
from .csv2htmltable import write_table
from .definitions import get_event_data, get_obs_data, goto_north, goto_south
from .slack_message import slackmessage

path = "./www"
send_email = False


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


def parse(event_data, all_obs_data, scope):
    """Parse an event for a given telescope."""
    name = event_data['name']
    event_type = event_data['type']
    trigger_id = event_data['trigger_id']
    contact = event_data['contact']

    scope_string = scope.name
    if scope_string == 'goto_north':
        obs_data = all_obs_data['north']
    elif scope_string == 'goto_south':
        obs_data = all_obs_data['south']

    # Check if the target rises above the horizon
    if obs_data["alt_observable"] is False:
        print("Target does not rise above alt 40 at {}".format(scope_string))
        return
    else:
        print("Target does rise above alt 40 at {}".format(scope_string))

    # Check if the target is visible for enough time
    if event_data["observation_end"] - event_data["observation_start"] < 1.5 * u.hour:
        print("Target is not up longer then 1:30 at {} during the night".format(scope_string))
        return
    else:
        print("Target is up longer then 1:30 at {} during the night".format(scope_string))

    # Check final constraint??
    if obs_data["final_constraint"] is False:
        print("Target does not rise above alt 40 at {} during observation peroid".format(
              scope_string))
        return
    else:
        print("Target does rise above alt 40 at {} during observation peroid".format(scope_string))

    # Find file paths
    file_name = name + trigger_id
    file_path = "./www/{}_transients/".format(scope_string)

    # Create graphs
    coms.create_graphs(event_data["event_coord"], scope, obs_data["airmass_time"],
                       file_path, file_name, 30, event_data["event_target"])

    # Write HTML
    title = "New transient for {} from {}".format(scope_string, name)
    coms.write_html(file_path, file_name, title, trigger_id, event_type,
                    event_data, obs_data, contact)

    # Send email if enabled
    email_subject = "Detection from {}".format(scope_string)
    email_body = "{} Detection: See more at http://118.138.235.166/~obrads".format(name)
    if send_email:
        coms.send_email(fromaddr="lapalmaobservatory@gmail.com",
                        toaddr="aobr10@student.monash.edu",
                        subject=email_subject,
                        body=email_body,
                        password="lapalmaobservatory1",
                        file_path=file_path,
                        file_name=file_name)

    # Write CSV
    csv_file = scope_string + ".csv"
    coms.write_csv(os.path.join(file_path, csv_file),
                   file_name,
                   event_data,
                   all_obs_data)

    # Write latest 10 page
    topten_file = "recent_ten.html"
    coms.write_topten(file_path, csv_file, topten_file)

    # Send message to Slack
    if scope_string == "goto_north":
        print("sent message to slack")
        slackmessage(name,
                     str(event_data["event_time"])[:22],
                     str(event_data["event_coord"].ra.deg),
                     str(event_data["event_coord"].dec.deg),
                     file_name)

    # Convert CSVs to HTML
    write_table(file_path, csv_file, 20)


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
        parse(event_data, all_obs_data, telescope)
        parse(event_data, all_obs_data, telescope)

    print("done")
