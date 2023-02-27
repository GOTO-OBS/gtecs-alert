#!/usr/bin/env python3
"""A simple test script for GOTO-alert."""

import importlib.resources as pkg_resources

from astropy import units as u

from gtecs.alert import params
from gtecs.alert.gcn import GCNNotice
from gtecs.alert.handler import handle_notice


if __name__ == '__main__':
    print('~~~~~~~~~~~~~~~')
    test_files = sorted([f for f in pkg_resources.contents('gtecs.alert.data.test_notices')
                         if f.endswith('.xml')])
    print('Found {} test notices:'.format(len(test_files)))
    for test_file in test_files:
        print(' - ', test_file)

    for test_file in test_files:
        print('~~~~~~~~~~~~~~~')
        with pkg_resources.path('gtecs.alert.data.test_notices', test_file) as f:
            print(f'Loading {f}')
            notice = GCNNotice.from_file(f)
        handle_notice(notice, send_messages=params.ENABLE_SLACK, time=notice.notice_time + 60 * u.s)
