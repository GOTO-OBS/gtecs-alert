"""Package parameters."""

import os

from gtecs.common.package import load_config, get_package_version


############################################################
# Load and validate config file
config, CONFIG_SPEC, CONFIG_FILE = load_config('alert', '.alert.conf')

############################################################
# Module parameters
VERSION = get_package_version('alert')

# Directory paths
FILE_PATH = config['FILE_PATH']
if FILE_PATH in ['path_not_set', '/path/goes/here/']:
    raise ValueError('FILE_PATH not set, check config file ({})'.format(CONFIG_FILE))
HTML_PATH = config['HTML_PATH']
if config['HTML_PATH'] in ['path_not_set', '/path/goes/here/']:
    HTML_PATH = os.path.join(FILE_PATH, 'html')

############################################################
# Filter parameters
IGNORE_ROLES = config['IGNORE_ROLES']

# Slack bot parameters
ENABLE_SLACK = config['ENABLE_SLACK']
SLACK_BOT_TOKEN = config['SLACK_BOT_TOKEN']
SLACK_DEFAULT_CHANNEL = config['SLACK_DEFAULT_CHANNEL']
