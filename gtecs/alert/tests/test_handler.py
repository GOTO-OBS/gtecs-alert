#!/usr/bin/env python3
"""A simple test script for GOTO-alert."""

import importlib.resources

from astropy import units as u

from gtecs.alert import params
from gtecs.alert.notices import Notice
from gtecs.alert.handler import handle_notice


if __name__ == '__main__':
    print('~~~~~~~~~~~~~~~')
    source_dirs = [
        source_dir
        for source_dir in importlib.resources.files('gtecs.alert.data.test_notices').iterdir()
        if source_dir.is_dir() and source_dir.name != '__pycache__'
    ]
    source_dirs = sorted(source_dirs)
    print(f'Found {len(source_dirs)} sources with test notices')
    for source_dir in source_dirs:
        print(' - ', source_dir.name)

    for source_dir in source_dirs:
        print('~~~~~~~~~~~~~~~')
        print(f'Loading {source_dir.name} test notices')
        notice_files = [
            notice_file for notice_file in source_dir.iterdir()
            if notice_file.is_file()
        ]
        notice_files = sorted(notice_files)
        print(f'Found {len(notice_files)} test notices:')
        for notice_file in notice_files:
            print(' - ', notice_file.name)

        for notice_file in notice_files:
            print('------------')
            print(f'Loading {notice_file}')
            notice = Notice.from_file(notice_file)
            print(notice)

            if notice.type == 'unknown':
                print('Skipping unknown notice')
                continue

            print('Handling notice')
            handle_notice(
                notice,
                send_messages=params.ENABLE_SLACK,
                time=notice.time + 60 * u.s,
            )
