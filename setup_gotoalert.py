#!/usr/bin/env python
"""A script to setup directory structure for GOTO-alert HTML files."""

import csv
import os
import shutil
import sys
import traceback

import pkg_resources


print('~~~~~~~~~~~~~~~~~~~~~~')
print('Setting up GOTO-alert')
print('~~~~~~~~~~~~~~~~~~~~~~')

# Check HTML path
HTML_PATH = './www'  # Should be in config file
print('HTML_PATH is set to: "{}"'.format(HTML_PATH))
print('')

# Create directories
try:
    if not os.path.exists(HTML_PATH):
        os.mkdir(HTML_PATH)
    print('Created ', HTML_PATH)

    for direc in ['goto_north_transients', 'goto_south_transients']:
        subpath = os.path.join(HTML_PATH, direc)
        if not os.path.exists(subpath):
            os.mkdir(subpath)
            print('Created ', subpath)

        for subdirec in ['airmass_plots', 'finder_charts']:
            subsubpath = os.path.join(subpath, subdirec)
            if not os.path.exists(subsubpath):
                os.mkdir(subsubpath)
                print('Created ', subsubpath)
except Exception:
    print('ERROR: Failed to create HTML directories')
    print('       Try creating {} yourself then re-running this script'.format(HTML_PATH))
    traceback.print_exc()
    sys.exit(1)
print('')

# Find package data files
data_dir = pkg_resources.resource_filename('gotoalert', 'data')

# Copy files to the new directories
try:
    shutil.copy(os.path.join(data_dir, 'index.html'),
                os.path.join(HTML_PATH, 'index.html'))
    print('Created ', os.path.join(HTML_PATH, 'index.html'))

    for direc in ['goto_north_transients', 'goto_south_transients']:
        subpath = os.path.join(HTML_PATH, direc)
        shutil.copy(os.path.join(data_dir, 'index2.html'),
                    os.path.join(subpath, 'index.html'))
        print('Created ', os.path.join(subpath, 'index.html'))

        shutil.copy(os.path.join(data_dir, 'recent_ten.html'),
                    os.path.join(subpath, 'recent_ten.html'))
        print('Created ', os.path.join(subpath, 'recent_ten.html'))

        shutil.copy(os.path.join(data_dir, 'template.html'),
                    os.path.join(subpath, 'template.html'))
        print('Created ', os.path.join(subpath, 'template.html'))
except Exception:
    print('ERROR: Failed to copy data files to HTML directories')
    traceback.print_exc()
    sys.exit(1)
print('')

# Create csv files
FIELDNAMES = ['trigger',
              'date',
              'ra',
              'dec',
              'Galactic Distance',
              'Galactic Lat',
              'goto north',
              'goto south',
              ]

try:
    for tel in ['goto_north', 'goto_south']:
        subpath = os.path.join(HTML_PATH, tel + '_transients')
        csvfile = os.path.join(subpath, tel + '.csv')
        if not os.path.exists(csvfile):
            with open(csvfile, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
            print('Created ', os.path.join(csvfile))
        else:
            with open(csvfile, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
except Exception:
    print('ERROR: Failed to create CSV files')
    traceback.print_exc()
    sys.exit(1)
print('')

print('Setup complete!')
