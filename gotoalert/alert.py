#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import os

import astropy.units as u
from astropy.time import Time

import numpy as np

import voeventparse as vp

from . import coms
from .csv2htmltable import write_table
from .definitions import event_definitions, goto_north, goto_south, observing_definitions
from .slack_message import slackmessage

path = "./www"
send_email = False


def parse(event_dictionary, site_dictionaries, scope):
    """Parse an event for a given telescope."""

    name = event_dictionary['name']
    event_type = event_dictionary['type']
    trigger_id = event_dictionary['trigger_id']
    contact = event_dictionary['contact']


    # Check if the event is too close to the galaxy
    if -8 < event_dictionary["object_galactic_lat"].value < 8:
        raise ValueError("too close to the Galactic plane")
    if event_dictionary["dist_galactic_center"].value < 15:
        raise ValueError("too close to the Galactic centre")

    scope_string = scope.name
    if scope_string == 'goto_north':
        site = site_dictionaries['north']
    elif scope_string == 'goto_south':
        site = site_dictionaries['south']

    # Check if the target rises above the horizon
    if site["alt_observable"] is False:
        print("Target does not rise above alt 40 at {}".format(scope_string))
        return
    else:
        print("Target does rise above alt 40 at {}".format(scope_string))

    # Check if the target is visible for enough time
    if event_dictionary["observation_end"] - event_dictionary["observation_start"] < 1.5 * u.hour:
        print("Target is not up longer then 1:30 at {} during the night".format(scope_string))
        return
    else:
        print("Target is up longer then 1:30 at {} during the night".format(scope_string))

    # Check final constraint??
    if site["final_constraint"] is False:
        print("Target does not rise above alt 40 at {} during observation peroid".format(
              scope_string))
        return
    else:
        print("Target does rise above alt 40 at {} during observation peroid".format(scope_string))

    # Find file paths
    file_name = name + trigger_id
    file_path = "./www/{}_transients/".format(scope_string)

    # Create graphs
    coms.create_graphs(event_dictionary["event_coord"], scope, site["airmass_time"],
                       file_path, file_name, 30, event_dictionary["event_target"])

    # Write HTML
    title = "New transient for {} from {}".format(scope_string, name)
    coms.write_html(file_path, file_name, title, trigger_id, event_type,
                    event_dictionary, site, contact)

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
                   event_dictionary,
                   site_dictionaries)

    # Write latest 10 page
    topten_file = "recent_ten.html"
    coms.write_topten(file_path, csv_file, topten_file)

    # Send message to Slack
    if scope_string == "goto_north":
        print("sent message to slack")
        slackmessage(name,
                     str(event_dictionary["event_time"])[:22],
                     str(event_dictionary["event_coord"].ra.deg),
                     str(event_dictionary["event_coord"].dec.deg),
                     file_name)

    # Convert CSVs to HTML
    write_table(file_path, csv_file, 20)


def event_handler(v):
    """Handle a VOEvent payload."""
    current_time = Time.now()

    # Get event data from the payload
    event_data = event_definitions(v, current_time)

    # Check role
    print('Event is marked as "{}"'.format(event_data['role']))
    if event_data['role'] in ['test', 'utility']:
        print('Ignoring {} event'.format(event_data['role']))
        return

    # Get alert name
    if event_data['type'] is None:
        print('Ignoring unrecognised event type: {}'.format(event_data['ivorn']))
        return
    print('Recognised event type: {} ({})'.format(event_data['name'], event_data['type']))

    # Get observing data
    telescopes = [goto_north(), goto_south()]
    obs_data = {}
    for telescope in telescopes:
        obs_data['north'] = observing_definitions(telescope, event_data)
        obs_data['south'] = observing_definitions(telescope, event_data)

    # write master csv file
    coms.write_csv(os.path.join(path, "master.csv"), event_data, obs_data)

    # parse the event for each site
    for telescope in telescopes:
        parse(event_data, obs_data, telescope)
        parse(event_data, obs_data, telescope)

    print("done")
