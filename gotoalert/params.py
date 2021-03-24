#!/usr/bin/env python
"""GOTO-alert module parameters."""

import os
import sys

import configobj

import pkg_resources

import validate

from .version import __version__


# Load configspec file for default configuration
if os.path.exists('gotoalert/data/configspec.ini'):
    # We are running in install dir, during installation
    CONFIGSPEC_FILE = 'gotoalert/data/configspec.ini'
else:
    # We are being imported, find pkg_resources
    CONFIGSPEC_FILE = pkg_resources.resource_filename('gotoalert', 'data/configspec.ini')

# Try to find .gotoalert.conf file, look in the home directory and
# anywhere specified by GOTOALERT_CONF environment variable
paths = [os.path.expanduser("~")]
if "GOTOALERT_CONF" in os.environ:
    GOTOALERT_CONF_PATH = os.environ["GOTOALERT_CONF"]
    paths.append(GOTOALERT_CONF_PATH)
else:
    GOTOALERT_CONF_PATH = None

# Load the config file as a ConfigObj
config = configobj.ConfigObj({}, configspec=CONFIGSPEC_FILE)
CONFIG_FILE_PATH = None
for loc in paths:
    try:
        with open(os.path.join(loc, ".gotoalert.conf")) as source:
            config = configobj.ConfigObj(source, configspec=CONFIGSPEC_FILE)
            CONFIG_FILE_PATH = loc
    except IOError:
        pass

# Validate ConfigObj, filling defaults from configspec if missing from config file
validator = validate.Validator()
result = config.validate(validator)
if result is not True:
    print('Config file validation failed')
    print([k for k in result if not result[k]])
    sys.exit(1)

############################################################
# Module parameters
VERSION = __version__

# Directory paths
FILE_PATH = config['FILE_PATH']
HTML_PATH = config['HTML_PATH']
if HTML_PATH == '/path/goes/here/':
    # Not set, default to FILE_PATH
    HTML_PATH = FILE_PATH

# Filter parameters
IGNORE_ROLES = config['IGNORE_ROLES']

# Slack bot parameters
ENABLE_SLACK = config['ENABLE_SLACK']
SLACK_BOT_TOKEN = config['SLACK_BOT_TOKEN']
SLACK_DEFAULT_CHANNEL = config['SLACK_DEFAULT_CHANNEL']
