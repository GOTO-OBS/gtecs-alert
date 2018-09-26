#! /opt/local/bin/python3.6
"""Event handlers for VOEvents."""

import os

from astropy.time import Time

import numpy as np

import voeventparse as vp

from . import coms
from .csv2htmltable import write_table
from .goto_observatories_definitions import event_definitions, observing_definitions, telescope
from .slack_message import slackmessage

path = "./www"
send_email = False


def parse(trigger_id, contact, event_dictionary, name, event_type, site_dictionaries,
          scope, scope_string):
    """Parse an event for a given telescope."""
    # Check if the event is too close to the galaxy
    if -8 < event_dictionary["object_galactic_lat"].value < 8:
        raise ValueError("too close to the Galactic plane")
    if event_dictionary["dist_galactic_center"].value < 15:
        raise ValueError("too close to the Galactic centre")

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
    if np.sum(scope.target_is_up(site["night_time"], event_dictionary["event_target"])) < 6:
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
    contact = v.Who.Author.contactEmail

    # First check role
    role = v.attrib['role']
    if role == "test":
        print('Event is marked as "test"')
        return
    if role == "utility":
        print('Event is marked as "utility"')
        return
    print('Event is marked as "{}"'.format(role))

    top_params = vp.get_toplevel_params(v)
    trigger_id = top_params['TrigID']['value']

    current_time = Time.now().utc
    event_dictionary = event_definitions(v, current_time)

    # Get alert name
    ivorn = event_dictionary["ivorn"]
    alert_dictionary = coms.alert_dictionary()
    if ivorn.startswith(alert_dictionary["Swift_XRT_POS"]):
        name = "Swift_XRT_POS_"
        event_type = str("swift")
    elif ivorn.startswith(alert_dictionary["Swift_BAT_GRB_POS"]):
        name = "Swift_BAT_GRB_POS_"
        event_type = str("swift")
    elif ivorn.startswith(alert_dictionary["Fermi_GMB_GND_POS"]):
        name = "Fermi_GMB_GND_POS_"
        event_type = str("fermi")
    else:
        # Something we don't care about
        print('unrecognised event: {}'.format(ivorn))
        return
    print('recognised event: {} ({})'.format(name, event_type))

    goto_north = telescope('goto north', +37, 145, 10, 'UTC')
    goto_south = telescope('goto south', -37, 145, 10, 'UTC')

    # get telescope definitions
    site_dictionaries = {}
    site_dictionaries['north'] = observing_definitions(goto_north, '23:59:59', 30,
                                                       event_dictionary)
    site_dictionaries['south'] = observing_definitions(goto_south, '11:59:59', 30,
                                                       event_dictionary)

    # write master csv file
    coms.write_csv(os.path.join(path, "master.csv"),
                   name + trigger_id,
                   event_dictionary,
                   site_dictionaries)

    # parse the event for each site
    parse(trigger_id, contact, event_dictionary, name, event_type,
          site_dictionaries, goto_north, "goto_north")
    parse(trigger_id, contact, event_dictionary, name, event_type,
          site_dictionaries, goto_south, "goto_south")

    print("done")
