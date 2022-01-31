#!/usr/bin/env python3
"""A script to setup directory structure for GOTO-alert HTML files."""

import csv
import glob
import os
import shutil
import sys
import traceback

try:
    from gtecs.alert import params
    gtecs_installed = True
except ModuleNotFoundError:
    gtecs_installed = False


SITES = ['goto_north', 'goto_south']

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('Setting up package data files')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

# Check the package has been installed
if not gtecs_installed:
    print('ERROR: Package not installed, run `pip3 install . --user first')
    sys.exit(1)

# Check for configuration file
if params.CONFIG_FILE is None:
    print('ERROR: No config file found')
    sys.exit(1)
print('Found config file at {}'.format(params.CONFIG_FILE))
print('')

# Check file path is set
if params.FILE_PATH in ['/path/goes/here/', 'path_not_set', None]:
    print('ERROR: FILE_PATH not set')
    print('       You need to edit the sample config file')
    sys.exit(1)
print('FILE_PATH is set to: "{}"'.format(params.FILE_PATH))
print('')

# Create directories
direcs = [params.FILE_PATH,
          params.HTML_PATH,
          params.LOG_PATH,
          ]
try:
    for direc in direcs:
        if not os.path.exists(direc):
            os.mkdir(direc)
            print('Created', direc)
        print('Checked', direc)

    for site in SITES:
        direc = os.path.join(params.HTML_PATH, site + '_transients')
        if not os.path.exists(direc):
            os.mkdir(direc)
            print('Created', direc)
        print('Checked', direc)
        for subdirec in ['airmass_plots', 'finder_charts']:
            direc = os.path.join(params.HTML_PATH, site + '_transients', subdirec)
            if not os.path.exists(direc):
                os.mkdir(direc)
                print('Created', direc)
            print('Checked', direc)
except Exception:
    print('ERROR: Failed to create directories')
    print('       Try creating {} yourself then re-running this script'.format(direc))
    traceback.print_exc()
    sys.exit(1)
print('')

# Copy sample data files to the new directories
try:
    files = glob.glob('data/*')
    for file in sorted([f for f in files if os.path.isfile(f)]):
        new_path = os.path.join(params.HTML_PATH, os.path.basename(file))
        if not os.path.exists(new_path):
            shutil.copy(file, new_path)
            print('Copied', file, 'to', params.HTML_PATH)
        else:
            print('Ignored existing', new_path)

    files = glob.glob('data/site_transients/*')
    for file in sorted([f for f in files if os.path.isfile(f)]):
        for site in SITES:
            new_path = os.path.join(params.HTML_PATH, site + '_transients', os.path.basename(file))
            if not os.path.exists(new_path):
                shutil.copy(file, new_path)
                print('Copied', file, 'to', params.HTML_PATH)
            else:
                print('Ignored existing', new_path)
except Exception:
    print('ERROR: Failed to copy data files')
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
    for site in SITES:
        new_path = os.path.join(params.HTML_PATH, site + '_transients', site + '.csv')
        if not os.path.exists(new_path):
            with open(new_path, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
            print('Created', new_path)
        else:
            print('Ignored existing', new_path)
except Exception:
    print('ERROR: Failed to create CSV files')
    traceback.print_exc()
    sys.exit(1)
print('')

print('Setup complete!')
