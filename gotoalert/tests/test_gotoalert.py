#!/usr/bin/env python
"""A simple test script for GOTO-alert."""

import os

from gotoalert.alert import event_handler
from gotoalert.events import Event

import pkg_resources


if __name__ == '__main__':
    # used for local testing
    data_path = pkg_resources.resource_filename('gotoalert', 'data')
    test_path = os.path.join(data_path, 'tests')
    for test_file in sorted(os.listdir(test_path)):
        print('~~~~~~~~~~~~~~~')
        with open(os.path.join(test_path, test_file), "rb") as f:
            payload = f.read()
            event = Event.from_payload(payload)
            event_handler(event)
