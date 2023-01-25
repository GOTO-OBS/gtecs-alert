"""Package parameters."""

import os

from gtecs.common import config as pkg_config
from gtecs.common.package import get_package_version, load_config


############################################################
# Load and validate config file
config, CONFIG_SPEC, CONFIG_FILE = load_config('alert', '.alert.conf')

############################################################
# Module parameters
VERSION = get_package_version('alert')

# Directory paths
FILE_PATH = pkg_config.CONFIG_PATH / 'alert'
HTML_PATH = config['HTML_PATH']
if config['HTML_PATH'] in ['path_not_set', '/path/goes/here/']:
    HTML_PATH = os.path.join(FILE_PATH, 'html')

############################################################
# Sentinel parameters
PYRO_HOST = config['PYRO_HOST']
PYRO_PORT = config['PYRO_PORT']
PYRO_URI = 'PYRO:sentinel@{}:{}'.format(PYRO_HOST, PYRO_PORT)
PYRO_TIMEOUT = config['PYRO_TIMEOUT']
LOCAL_IVO = config['LOCAL_IVO']
VOSERVER_HOST = config['VOSERVER_HOST']
VOSERVER_PORT = config['VOSERVER_PORT']
KAFKA_CLIENT_ID = config['KAFKA_CLIENT_ID']
KAFKA_CLIENT_SECRET = config['KAFKA_CLIENT_SECRET']

# Filter parameters
IGNORE_ROLES = config['IGNORE_ROLES']

# Slack bot parameters
ENABLE_SLACK = config['ENABLE_SLACK']
SLACK_BOT_TOKEN = config['SLACK_BOT_TOKEN']
SLACK_DEFAULT_CHANNEL = config['SLACK_DEFAULT_CHANNEL']
