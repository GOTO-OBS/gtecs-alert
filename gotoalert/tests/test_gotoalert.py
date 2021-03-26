#!/usr/bin/env python3
"""A simple test script for GOTO-alert."""

import os

from gotoalert import params
from gotoalert.alert import event_handler
from gotoalert.events import Event

import pkg_resources


if __name__ == '__main__':
    # used for local testing
    data_path = pkg_resources.resource_filename('gotoalert', 'data')
    test_path = os.path.join(data_path, 'tests')
    for test_file in sorted(os.listdir(test_path)):
        print('~~~~~~~~~~~~~~~')
        filepath = os.path.join(test_path, test_file)
        event = Event.from_file(filepath)
        event_handler(event, send_messages=params.ENABLE_SLACK)
