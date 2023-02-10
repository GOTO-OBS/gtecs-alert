#!/usr/bin/env python3
"""A simple test script for GOTO-alert."""

import importlib.resources as pkg_resources

from astropy import units as u

from gtecs.alert import params
from gtecs.alert.events import Event
from gtecs.alert.handler import handle_event


if __name__ == '__main__':
    print('~~~~~~~~~~~~~~~')
    test_files = sorted([f for f in pkg_resources.contents('gtecs.alert.data.test_events')
                         if f.endswith('.xml')])
    print('Found {} test events:'.format(len(test_files)))
    for test_file in test_files:
        print(' - ', test_file)

    for test_file in test_files:
        print('~~~~~~~~~~~~~~~')
        with pkg_resources.path('gtecs.alert.data.test_events', test_file) as f:
            print(f'Loading {f}')
            event = Event.from_file(f)
        handle_event(event, send_messages=params.ENABLE_SLACK, time=event.notice_time + 60 * u.s)
