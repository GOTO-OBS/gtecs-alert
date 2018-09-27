#! /opt/local/bin/python3.6

import csv
import os
import smtplib
from collections import OrderedDict
from decimal import Decimal
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from astroplan.plots import dark_style_sheet, plot_airmass, plot_finder_image

import astropy.units as u

import matplotlib.pyplot as plt

import numpy as np

import pandas as pd


def write_csv(filename, event_data, all_obs_data):
    """Write the CSV file."""
    data = OrderedDict()
    data['trigger'] = event_data['name'] + event_data['trigger_id']
    data['date'] = event_data["event_time"]
    data['ra'] = event_data["event_coord"].ra.deg
    data['dec'] = event_data["event_coord"].dec.deg
    data['Galactic Distance'] = event_data["dist_galactic_center"]
    data['Galactic Lat'] = event_data["object_galactic_lat"]

    for telescope in all_obs_data:
        obs_data = all_obs_data[telescope]
        data[telescope] = obs_data['alt_observable']

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


def create_graphs(file_path, event_data, obs_data, fov=30):
    """Create airmass and finder plots."""

    # Get data
    name = event_data['name']
    trigger_id = event_data['trigger_id']
    coord = event_data['event_coord']
    target = event_data['event_target']
    observer = obs_data['observer']

    # Plot airmass during the night
    delta_t = obs_data['sun_rise'] - obs_data['sun_set']
    time_range = obs_data['sun_set'] + delta_t * np.linspace(0, 1, 75)
    plot_airmass(coord, observer, time_range, altitude_yaxis=True, style_sheet=dark_style_sheet)
    airmass_file = "{}{}_AIRMASS.png".format(name, trigger_id)
    plt.savefig(os.path.join(file_path, 'airmass_plots', airmass_file))
    plt.clf()

    # Plot finder chart
    plot_finder_image(target, fov_radius=fov * u.arcmin, grid=False, reticle=True)
    airmass_file = "{}{}_FINDER.png".format(name, trigger_id)
    plt.savefig(os.path.join(file_path, 'finder_charts', airmass_file))
    plt.clf()


def write_html(file_path, file_name, title, trigger_id, event_type, event_data, obs_data, email):
    """Write the HTML page."""
    eventtime = event_data["event_time"]
    coord = event_data["event_coord"]
    error = event_data["event_coord_error"]
    dist = event_data["dist_galactic_center"],
    object_galactic_lat = event_data["object_galactic_lat"]

    target_rise = obs_data["target_rise"]
    target_set = obs_data["target_set"]
    dark_sunset_tonight = obs_data["dark_sunset_tonight"]
    dark_sunrise_tonight = obs_data["dark_sunrise_tonight"]
    observation_start = obs_data["observation_start"]
    observation_end = obs_data["observation_end"]
    moon = obs_data["moon_observable"]

    html_file = file_name + '.html'
    with open(file_path + html_file, 'w') as f:
        f.write('<!DOCTYPE html><html lang="en"><head>{}</head><body>'.format(title))
        f.write('<p>https://gcn.gsfc.nasa.gov/other/{}.{}</p>'.format(trigger_id, event_type))
        f.write('<p>Event ID:  {}</p>'.format(trigger_id))
        f.write('<p>Time of event (UTC): {}</p>'.format(str(eventtime)[:21]))
        f.write('<p>RA:  {} degrees</p>'.format(str(coord.ra.deg)))
        f.write('<p>DEC: {} degrees</p>'.format(str(coord.dec.deg)))
        f.write('<p>RA, DEC Error:   {}</p>'.format(str('{:.10f}'.format(Decimal(error))[:5])))
        f.write('<p>Contact: {}</p>'.format(email))
        f.write('<p>Observation Details: Time in UTC</p>')
        f.write('<p>Target Rise: {}</p>'.format(str((target_rise.iso))[:19]))
        f.write('<p>Target Set:  {}</p>'.format(str((target_set.iso))[:19]))
        f.write('<p>Start of night:  {}</p>'.format(str((dark_sunset_tonight.iso))[:19]))
        f.write('<p>End of night:    {}</p>'.format(str((dark_sunrise_tonight.iso))[:19]))
        f.write('<p>Observations Start:   {}</p>'.format(str((observation_start.iso))[:19]))
        f.write('<p>Observations End:  {}</p>'.format(str((observation_end.iso))[:19]))
        f.write('<p>Galactic Distance:   {} degrees</p>'.format(str(dist.value)[:6]))
        f.write('<p>Galactic Lat:    {} degrees</p>'.format(str(object_galactic_lat.value)[:6]))
        f.write('<p>Target within 5 degrees of the moon? {}</p>'.format(str(not moon)))
        f.write('<img src=finder_charts/{}_FINDER.png>'.format(file_name))
        f.write('<img src=airmass_plots/{}_AIRMASS.png>'.format(file_name))
        f.write('</body></html>')


def send_email(fromaddr, toaddr, subject, body, password, file_path, file_name):
    """Send an email when an event is detected."""
    # Create message
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Attach HTML file
    html_file = file_name + '.html'
    with open(file_path + html_file, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename={}'.format(html_file))
    msg.attach(part)

    # Connect to server and send
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, password)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def write_topten(file_path, csv_file, topten_file):
    """Write the latest 10 events page."""
    csv_path = os.path.expanduser(file_path + csv_file)
    df = pd.read_csv(csv_path)

    # sort by date, pick the latest 10 and write to HTML
    df = df.sort_values('date')[-10:]
    html_table = df.to_html()

    with open(file_path + topten_file, 'w') as f:
        f.write('<!DOCTYPE html><html lang="en"><head>Recent Events</head><body>')
        f.write('<p>{}</p>'.format(html_table))
