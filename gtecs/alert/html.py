"""Functions to write output HTML pages."""

import csv
import os
from collections import OrderedDict

from astroplan import FixedTarget
from astroplan.plots import dark_style_sheet, plot_airmass, plot_finder_image

import astropy.units as u
from astropy.coordinates import SkyCoord

import matplotlib.pyplot as plt

import numpy as np

import pandas as pd


def format_desc(row, gaialink):
    """Format the description with a link to the Gaia website."""
    if row['trigger'].lower().startswith('gaia'):
        return '<a href="{gaialink}{trigger}">{desc}</a>'.format(
            gaialink=gaialink, trigger=row['trigger'], desc=row['description'])
    return ""


def parse(df, ntrigs=20):
    """Sort the Pandas table, format the link, and select the top ntrigs."""
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date', ascending=False)
    df = df[:ntrigs]
    df['trigger'] = df['trigger'].apply(
        lambda x: '<a href="{trigger}.html">{trigger}</a>'.format(trigger=x))
    return df


def format_template(df, file_path):
    """Read a HTML template, insert the CSV HTML table, write index.html."""
    template_file = os.path.join(file_path, "template.html")
    with open(template_file) as f:
        html = f.read()

    pd.set_option('display.max_colwidth', -1)
    table = df.to_html(classes=['table', 'table-striped', 'table-hover'],
                       index=False, escape=False)
    html = html.replace('{{ transients_table }}', table)

    index_file = os.path.join(file_path, "index.html")
    with open(index_file, 'w') as f:
        f.write(html)


def write_table(file_path, csv_file, ntrigs=20):
    """Convert the CSV table into HTML."""
    df = pd.read_csv(os.path.join(file_path, csv_file))
    df = parse(df, ntrigs)
    format_template(df, file_path)


def write_csv(filename, notice, obs_data):
    """Write the CSV file."""
    data = OrderedDict()
    data['trigger'] = notice.event_name
    data['date'] = notice.event_time
    data['ra'] = notice.position.ra.deg
    data['dec'] = notice.position.dec.deg
    galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
    data['Galactic Distance'] = notice.position.galactic.separation(galactic_center).value
    data['Galactic Lat'] = notice.position.galactic.b.value

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


def create_graphs(file_path, notice, site_data, fov=30):
    """Create airmass and finder plots."""
    # Plot airmass during the night
    delta_t = site_data['sun_rise'] - site_data['sun_set']
    time_range = site_data['sun_set'] + delta_t * np.linspace(0, 1, 75)
    plot_airmass(notice.position, site_data['observer'], time_range, altitude_yaxis=True,
                 style_sheet=dark_style_sheet)

    plots_path = os.path.join(file_path, 'airmass_plots')
    if not os.path.exists(plots_path):
        os.mkdir(plots_path)
    plt.savefig(os.path.join(plots_path, '{}_AIRMASS.png'.format(notice.event_name)))
    plt.clf()

    # Plot finder chart
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        target = FixedTarget(coord=notice.position)
        plot_finder_image(target, fov_radius=fov * u.arcmin, grid=False, reticle=True)

    finder_path = os.path.join(file_path, 'finder_charts')
    if not os.path.exists(finder_path):
        os.mkdir(finder_path)
    plt.savefig(os.path.join(finder_path, '{}_FINDER.png'.format(notice.event_name)))
    plt.clf()


def write_html(file_path, notice, site_data):
    """Write the HTML page."""
    site_name = site_data['observer'].name

    html_file = '{}.html'.format(notice.event_name)
    html_path = os.path.join(file_path, html_file)
    with open(html_path, 'w') as f:

        title = "New transient for {} from {} notice".format(site_name, notice.type)
        f.write('<!DOCTYPE html><html lang="en"><head>{}</head><body>'.format(title))

        page = '{}.{}'.format(notice.event_id, notice.source.lower())
        f.write('<p>https://gcn.gsfc.nasa.gov/other/{}</p>'.format(page))
        f.write('<p>Event ID:  {}</p>'.format(notice.event_id))

        # Write event time
        event_time = notice.event_time
        f.write('<p>Time of event (UTC): {}</p>'.format(event_time))

        # Write event coords
        f.write('<p>RA:  {:.3f} degrees</p>'.format(notice.position.ra.deg))
        f.write('<p>DEC: {:.3f} degrees</p>'.format(notice.position.dec.deg))
        f.write('<p>RA, DEC Error:   {:.3f}</p>'.format(notice.position_error.deg))

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

        # Write galactic details
        galactic_center = SkyCoord(l=0, b=0, unit='deg,deg', frame='galactic')
        gal_dist = notice.position.galactic.separation(galactic_center).value
        gal_lat = notice.position.galactic.b.value
        f.write('<p>Galactic Distance:   {:.3f} degrees</p>'.format(gal_dist))
        f.write('<p>Galactic Lat:    {:.3f} degrees</p>'.format(gal_lat))

        # Write obs check
        near_moon = not site_data["moon_observable"]
        f.write('<p>Target within 5 degrees of the moon? {}</p>'.format(near_moon))

        # Write links to plots
        f.write('<img src=finder_charts/{}_FINDER.png>'.format(notice.event_name))
        f.write('<img src=airmass_plots/{}_AIRMASS.png>'.format(notice.event_name))
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


def create_webpages(notice, obs_data, site_name, web_path):
    """Create the output webpages for the given telescope."""
    site_data = obs_data[site_name]

    # write master csv file
    write_csv(os.path.join(web_path, 'master.csv'), notice, obs_data)

    # Find file paths
    web_directory = '{}_transients'.format(site_name)
    file_path = os.path.join(web_path, web_directory)
    if not os.path.exists(file_path):
        os.mkdir(file_path)

    # Create graphs
    create_graphs(file_path, notice, site_data)

    # Write HTML
    write_html(file_path, notice, site_data)

    # Write CSV
    csv_file = site_name + ".csv"
    write_csv(os.path.join(file_path, csv_file), notice, obs_data)

    # Write latest 10 page
    topten_file = "recent_ten.html"
    write_topten(os.path.join(file_path, csv_file), os.path.join(file_path, topten_file))

    # Convert CSVs to HTML
    write_table(file_path, csv_file)
