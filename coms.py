#! /opt/local/bin/python3.6
import os
import csv
from astroplan.plots import dark_style_sheet, plot_airmass, plot_finder_image
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import astropy.units as u
from decimal import Decimal
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

def alert_dictionary():
    data = {
    "gaia": "ivo://gaia.cam.uk/alerts#",
    "Swift_XRT_POS": "ivo://nasa.gsfc.gcn/SWIFT#XRT_Pos",
    "Swift_BAT_GRB_POS": "ivo://nasa.gsfc.gcn/SWIFT#BAT_GRB_Pos",
    "Fermi_GMB_GND_POS": "ivo://nasa.gsfc.gcn/Fermi#GBM_Gnd_Pos_"
    }
    return data

def writecsv(
    filename,
    ivorn,
    trigger,
    date,
    ra,
    dec,
    gal_dis,
    gal_lat,
    obs_north,
    obs_south,
    ):

    FIELDNAMES = [
        'trigger',
        'date',
        'ra',
        'dec',
        'Galactic Distance',
        'Galactic Lat',
        'goto north',
        'goto south',
        ]

    data = {
        'trigger': trigger,
        'date': date,
        'ra': ra,
        'dec': dec,
        'Galactic Distance': gal_dis,
        'Galactic Lat': gal_lat,
        'goto north': obs_north,
        'goto south': obs_south,
    }

    if not os.path.exists(filename):
        with open(filename, 'w') as fp:
            writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerow(data)
    else:
        with open(filename, 'a') as fp:
            writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)
            writer.writerow(data)



def create_graphs(
    coord,
    telescope,
    airmass_time,
    file_path1,
    file_path2,
    file_name,
    deg,
    eventcoord
    ):

    plot_airmass(coord, telescope, airmass_time, altitude_yaxis=True,  style_sheet=dark_style_sheet)
    plt.savefig(file_path1+file_name+"_AIRMASS.png")
    plt.clf()

    ax, hdu = plot_finder_image(eventcoord, fov_radius=deg*u.arcmin, grid=False, reticle=True)
    plt.savefig(file_path2+file_name+"_FINDER.png")
    plt.clf()


def htmlwrite(
    file_path3,
    file_name,
    title,
    id,
    type,
    eventtime,
    coord,
    error,
    email,
    target_rise,
    target_set,
    dark_sunset_tonight,
    dark_sunrise_tonight,
    observation_start,
    observation_end,
    dist,
    object_galactic_lat,
    ):

    text_file = open(file_path3+file_name+".html", 'w')
    text_file.write('<!DOCTYPE html><html lang="en"><head>'+title+'</head><body>')
    text_file.write('<p>'+"https://gcn.gsfc.nasa.gov/other/"+id+"."+type+'</p>')
    text_file.write('<p>'+'Event ID:'+"  "+id+'</p>')
    text_file.write('<p>'+"Time of event (UTC): "+str(eventtime)[:21]+'</p>')
    text_file.write('<p>'+'RA:  '+str(coord.ra.deg)+" degrees"'</p>')
    text_file.write('<p>'+"DEC: "+str(coord.dec.deg)+" degrees"'</p>')
    text_file.write('<p>'+'RA, DEC Error:   '+str('%.10f'%Decimal(error))[:5]+'</p>')
    text_file.write('<p>'+"Contact: "+email+'</p>')
    text_file.write('<p>'+'Observation Details: Time in UTC'+'</p>')
    text_file.write('<p>'+"Target Rise: "+str((target_rise.iso))[:19]+'</p>')
    text_file.write('<p>'+'Target Set:  '+str((target_set.iso))[:19]+'</p>')
    text_file.write('<p>'+'Start of night:  '+str((dark_sunset_tonight.iso))[:19]+'</p>')
    text_file.write('<p>'+'End of night:    '+str((dark_sunrise_tonight.iso))[:19]+'</p>')
    text_file.write('<p>'+'Observations Start:   '+str((observation_start.iso))[:19]+'</p>')
    text_file.write('<p>'+'Observations End:  '+str((observation_end.iso))[:19]+'</p>')
    text_file.write('<p>'+'Galactic Distance:   '+str(dist.value)[:6]+" degrees"'</p>')
    text_file.write('<p>'+'Galactic Lat:    '+str(object_galactic_lat.value)[:6]+" degrees"'</p>')
    text_file.write("<img src=finder_charts/"+file_name+"_FINDER.png>")
    text_file.write("<img src=airmass_plots/"+file_name+"_AIRMASS.png>")
    text_file.write('</body></html>')
    text_file.close()


def sendemail(
    fromemail,
    toaddress,
    subject,
    bodymessage,
    password,
    file_path,
    file_name
    ):

    fromaddr = fromemail
    toaddr = toaddress

    msg = MIMEMultipart()

    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject

    body = bodymessage

    msg.attach(MIMEText(body, 'plain'))

    attachment = open(file_path+file_name+".html", "rb")

    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % file_name+".html")

    msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, password)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def topten(
    path1,
    path2
    ):

    path5 = os.path.expanduser(path1)

    df = pd.read_csv(path5)
    # sort by date
    df = df.sort_values('date')
    # pick the last 10
    df = df[-10:]
    # write the dataframe to a HTML table string
    html_table = df.to_html()

    text_file = open(path2, 'w')
    text_file.write('<!DOCTYPE html><html lang="en"><head>'+"Recent Events"+'</head><body>')
    text_file.write('<p>'+html_table+'</p>')
    text_file.close()
