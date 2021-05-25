#!/usr/bin/env python3
"""A simple test script for GOTO-alert."""

from gtecs.alert import params
from gtecs.alert.events import Event
from gtecs.alert.handler import event_handler

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Python < 3.7
    import importlib_resources as pkg_resources  # type: ignore


if __name__ == '__main__':
    # used for local testing
    for test_file in sorted(pkg_resources.contents('gtecs.alert.data.test_events')):
        print('~~~~~~~~~~~~~~~')
        event = Event.from_file(test_file)
        event_handler(event, send_messages=params.ENABLE_SLACK)
