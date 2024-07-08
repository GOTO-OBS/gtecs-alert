"""Package parameters."""

import os

from gtecs.common import config as pkg_config
from gtecs.common.package import get_package_version, load_config
from gtecs.common.system import get_local_ip


############################################################
# Load and validate config file
config, CONFIG_SPEC, CONFIG_FILE = load_config('alert', '.alert.conf')

############################################################
# Module parameters
VERSION = get_package_version('alert')

# General parameters
LOCAL_HOST = get_local_ip()

# Directory paths
FILE_PATH = pkg_config.CONFIG_PATH / 'alert'
HTML_PATH = config['HTML_PATH']
if config['HTML_PATH'] in ['path_not_set', '/path/goes/here/']:
    HTML_PATH = os.path.join(FILE_PATH, 'html')

############################################################
# Sentinel parameters
PYRO_HOST = config['PYRO_HOST']
if PYRO_HOST == 'localhost':
    PYRO_HOST = LOCAL_HOST
PYRO_PORT = config['PYRO_PORT']
PYRO_URI = 'PYRO:sentinel@{}:{}'.format(PYRO_HOST, PYRO_PORT)
PYRO_TIMEOUT = config['PYRO_TIMEOUT']
LOCAL_IVO = config['LOCAL_IVO']
VOSERVER_HOST = config['VOSERVER_HOST']
VOSERVER_PORT = config['VOSERVER_PORT']
KAFKA_USER = config['KAFKA_USER']
KAFKA_PASSWORD = config['KAFKA_PASSWORD']
KAFKA_BROKER = config['KAFKA_BROKER']
KAFKA_TOPICS = config['KAFKA_TOPICS']
KAFKA_GROUP_ID = config['KAFKA_GROUP_ID']
KAFKA_BACKDATE = config['KAFKA_BACKDATE']

# Filter parameters
# TODO: Couldn't this be a switchable flag within the sentinel?
PROCESS_TEST_NOTICES = config['PROCESS_TEST_NOTICES']

############################################################
# Database parameters
DATABASE_USER = config['DATABASE_USER']
DATABASE_PASSWORD = config['DATABASE_PASSWORD']
DATABASE_HOST = config['DATABASE_HOST']
DATABASE_ECHO = bool(config['DATABASE_ECHO'])
DATABASE_PRE_PING = bool(config['DATABASE_PRE_PING'])

############################################################
# Slack bot parameters
ENABLE_SLACK = bool(config['ENABLE_SLACK'])
SLACK_BOT_TOKEN = config['SLACK_BOT_TOKEN']

SLACK_DEFAULT_CHANNEL = config['SLACK_DEFAULT_CHANNEL']
if SLACK_DEFAULT_CHANNEL == 'none':
    SLACK_DEFAULT_CHANNEL = None
SLACK_WAKEUP_CHANNEL = config['SLACK_WAKEUP_CHANNEL']
if SLACK_WAKEUP_CHANNEL == 'none':
    SLACK_WAKEUP_CHANNEL = None
SLACK_IGNORED_CHANNEL = config['SLACK_IGNORED_CHANNEL']
if SLACK_IGNORED_CHANNEL == 'none':
    SLACK_IGNORED_CHANNEL = None
SLACK_EVENT_CHANNELS = config['SLACK_EVENT_CHANNELS']
for event_type in SLACK_EVENT_CHANNELS:
    if SLACK_EVENT_CHANNELS[event_type] == 'none':
        SLACK_EVENT_CHANNELS[event_type] = None
