"""Package parameters."""

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


# Load configspec file for default configuration
CONFIGSPEC = pkg_resources.read_text('gtecs.alert.data', 'configspec.ini').split('\n')
config = configobj.ConfigObj({}, configspec=CONFIGSPEC)

# Try to find the config file, look in the home directory and
# anywhere specified by GTECS_CONF environment variable
CONFIG_FILE = '.alert.conf'
home = os.path.expanduser('~')
paths = [home, os.path.join(home, 'gtecs'), os.path.join(home, '.gtecs')]
if 'GTECS_CONF' in os.environ:
    paths.append(os.environ['GTECS_CONF'])

# Load the config file as a ConfigObj
CONFIG_FILE_PATH = None
for loc in paths:
    try:
        with open(os.path.join(loc, CONFIG_FILE)) as source:
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
if FILE_PATH in ['path_not_set', '/path/goes/here/']:
    raise ValueError('FILE_PATH not set, check your {} file'.format(CONFIG_FILE))
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
