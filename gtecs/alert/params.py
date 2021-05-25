"""GOTO-alert module parameters."""

import os
import sys

import configobj

from importlib.metadata import version
try:
    import importlib.resources as pkg_resources
except ImportError:
    # Python < 3.7
    import importlib_resources as pkg_resources  # type: ignore

import validate


# Try to find .gotoalert.conf file, look in the home directory and
# anywhere specified by GOTOALERT_CONF environment variable
paths = [os.path.expanduser('~')]
if "GOTOALERT_CONF" in os.environ:
    GOTOALERT_CONF_PATH = os.environ["GOTOALERT_CONF"]
    paths.append(GOTOALERT_CONF_PATH)
else:
    GOTOALERT_CONF_PATH = None

# Load configspec file for default configuration
CONFIGSPEC = pkg_resources.read_text('gtecs.alert.data', 'configspec.ini').split('\n')

# Load the config file as a ConfigObj
config = configobj.ConfigObj({}, configspec=CONFIGSPEC)
CONFIG_FILE_PATH = None
for loc in paths:
    try:
        with open(os.path.join(loc, '.gotoalert.conf')) as source:
            config = configobj.ConfigObj(source, configspec=CONFIGSPEC)
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
VERSION = version('gtecs-alert')

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
