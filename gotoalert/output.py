#! /opt/local/bin/python3.6
"""Functions to write output HTML pages."""

import csv
import os
from collections import OrderedDict

from astroplan.plots import dark_style_sheet, plot_airmass, plot_finder_image

import astropy.units as u

import matplotlib.pyplot as plt

import numpy as np

import pandas as pd

from .csv2htmltable import write_table


def write_csv(filename, event_data, obs_data):
    """Write the CSV file."""
    data = OrderedDict()
    data['trigger'] = event_data['event_name']
    data['date'] = event_data["event_time"]
    data['ra'] = event_data["event_coord"].ra.deg
    data['dec'] = event_data["event_coord"].dec.deg
    data['Galactic Distance'] = event_data["dist_galactic_center"]
    data['Galactic Lat'] = event_data["object_galactic_lat"]

    for site_name in obs_data:
        site_data = obs_data[site_name]
        data[site_name] = site_data['alt_observable']

    fieldnames = list(data.keys())

    # Write the data
    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerow(data)
    else:
        with open(filename, 'a') as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writerow(data)


def create_graphs(file_path, event_data, site_data, fov=30):
    """Create airmass and finder plots."""
    # Get data
    event_name = event_data['event_name']
    coord = event_data['event_coord']
    target = event_data['event_target']
    observer = site_data['observer']

    # Plot airmass during the night
    delta_t = site_data['sun_rise'] - site_data['sun_set']
    time_range = site_data['sun_set'] + delta_t * np.linspace(0, 1, 75)
    plot_airmass(coord, observer, time_range, altitude_yaxis=True, style_sheet=dark_style_sheet)
    airmass_file = "{}_AIRMASS.png".format(event_name)
    plt.savefig(os.path.join(file_path, 'airmass_plots', airmass_file))
    plt.clf()

    # Plot finder chart
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        plot_finder_image(target, fov_radius=fov * u.arcmin, grid=False, reticle=True)
    airmass_file = "{}_FINDER.png".format(event_name)
    plt.savefig(os.path.join(file_path, 'finder_charts', airmass_file))
    plt.clf()


def write_html(file_path, event_data, site_data):
    """Write the HTML page."""
    event_name = event_data['event_name']
    trigger_id = event_data['trigger_id']
    source = event_data['source']

    site_name = site_data['observer'].name

    html_file = '{}.html'.format(event_name)
    html_path = os.path.join(file_path, html_file)
    with open(html_path, 'w') as f:

        title = "New transient for {} from {}".format(site_name, event_data['base_name'])
        f.write('<!DOCTYPE html><html lang="en"><head>{}</head><body>'.format(title))
        f.write('<p>https://gcn.gsfc.nasa.gov/other/{}.{}</p>'.format(trigger_id, source.lower()))
        f.write('<p>Event ID:  {}</p>'.format(trigger_id))

        # Write event time
        event_time = event_data["event_time"].iso
        f.write('<p>Time of event (UTC): {}</p>'.format(event_time))

        # Write event coords
        coord = event_data["event_coord"]
        error = event_data["event_coord_error"]
        f.write('<p>RA:  {:.3f} degrees</p>'.format(coord.ra.deg))
        f.write('<p>DEC: {:.3f} degrees</p>'.format(coord.dec.deg))
        f.write('<p>RA, DEC Error:   {:.3f}</p>'.format(error))

        # Write event contact
        contact = event_data['contact']
        f.write('<p>Contact: {}</p>'.format(contact))

        # Write obs details
        f.write('<p>Observation Details: Time in UTC</p>')

        # Write obs times
        target_rise = site_data["target_rise"].iso
        target_set = site_data["target_set"].iso
        sun_set = site_data["sun_set"].iso
        sun_rise = site_data["sun_rise"].iso
        observation_start = site_data["observation_start"].iso
        observation_end = site_data["observation_end"].iso
        f.write('<p>Target Rise: {}</p>'.format(target_rise))
        f.write('<p>Target Set:  {}</p>'.format(target_set))
        f.write('<p>Start of night:  {}</p>'.format(sun_set))
        f.write('<p>End of night:    {}</p>'.format(sun_rise))
        f.write('<p>Observations Start:   {}</p>'.format(observation_start))
        f.write('<p>Observations End:  {}</p>'.format(observation_end))

        # Write obs checks
        galactic_dist = event_data["dist_galactic_center"]
        galactic_lat = event_data["object_galactic_lat"]
        moon = not site_data["moon_observable"]

        f.write('<p>Galactic Distance:   {:.3f} degrees</p>'.format(galactic_dist))
        f.write('<p>Galactic Lat:    {:.3f} degrees</p>'.format(galactic_lat))
        f.write('<p>Target within 5 degrees of the moon? {}</p>'.format(moon))

        # Write links to plots
        f.write('<img src=finder_charts/{}_FINDER.png>'.format(event_name))
        f.write('<img src=airmass_plots/{}_AIRMASS.png>'.format(event_name))
        f.write('</body></html>')


def write_topten(csv_path, topten_path):
    """Write the latest 10 events page."""
    # Load the CSV file
    df = pd.read_csv(csv_path)

    # sort by date, pick the latest 10 and write to HTML
    df = df.sort_values('date')[-10:]
    html_table = df.to_html()

    with open(topten_path, 'w') as f:
        f.write('<!DOCTYPE html><html lang="en"><head>Recent Events</head><body>')
        f.write('<p>{}</p>'.format(html_table))


def create_webpages(event_data, obs_data, site_name, web_path):
    """Create the output webpages for the given telescope."""
    site_data = obs_data[site_name]

    # write master csv file
    write_csv(os.path.join(web_path, 'master.csv'), event_data, obs_data)

    # Find file paths
    web_directory = '{}_transients'.format(site_name)
    file_path = os.path.join(web_path, web_directory)

    # Create graphs
    create_graphs(file_path, event_data, site_data)

    # Write HTML
    write_html(file_path, event_data, site_data)

    # Write CSV
    csv_file = site_name + ".csv"
    write_csv(os.path.join(file_path, csv_file), event_data, obs_data)

    # Write latest 10 page
    topten_file = "recent_ten.html"
    write_topten(os.path.join(file_path, csv_file), os.path.join(file_path, topten_file))

    # Convert CSVs to HTML
    write_table(file_path, csv_file)
