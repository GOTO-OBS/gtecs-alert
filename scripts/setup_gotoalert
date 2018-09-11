#!/usr/bin/env python

import sys
import os
import csv
import pkg_resources
import shutil


# create directories
d = "./www"
d1 = "./www/goto_north_transients"
d2 = "./www/goto_north_transients/airmass_plots"
d3 = "./www/goto_north_transients/finder_charts"
d4 = "./www/goto_south_transients"
d5 = "./www/goto_south_transients/airmass_plots"
d6 = "./www/goto_south_transients/finder_charts"
d9 = "./logs"
d7 = "./www/goto_south_transients/goto_south.csv"
d8 = "./www/goto_north_transients/goto_north.csv"

directories = [d, d1, d2, d3, d4, d5, d6, d9]
try:
    for folder in directories:
        if not os.path.exists(folder):
            os.makedirs(folder)
except OSError:
    print("Error: Creating directory.")

# copy data files
data_dir = pkg_resources.resource_filename('gotoalert', 'data')

shutil.copy(os.path.join(data_dir, 'index.html'), os.path.join(d, 'index.html'))

for direc in [d1, d4]:
    shutil.copy(os.path.join(data_dir, 'index2.html'), os.path.join(direc, 'index.html'))
    shutil.copy(os.path.join(data_dir, 'recent_ten.html'), os.path.join(direc, 'recent_ten.html'))
    shutil.copy(os.path.join(data_dir, 'template.html'), os.path.join(direc, 'template.html'))

# create csv files
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

if not os.path.exists(d7):
    with open(d7, 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)
        writer.writeheader()

else:
    with open(d7, 'a') as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)

if not os.path.exists(d8):
    with open(d8, 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)
        writer.writeheader()

else:
    with open(d8, 'a') as fp:
        writer = csv.DictWriter(fp, fieldnames=FIELDNAMES)
