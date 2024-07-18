#!/usr/bin/env python3
"""A simple test script for the GOTO-alert database."""

import importlib.resources

from gtecs.alert import database as db
from gtecs.alert.notices import Notice


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
            notice.get_skymap()
            print(notice)

            if notice.type == 'unknown':
                print('Skipping unknown notice')
                continue

            print('Adding to database')
            with db.session_manager() as session:
                db_notice = db.Notice.from_gcn(notice)
                session.add(db_notice)

            print('Loading from database')
            with db.session_manager() as session:
                db_notice = session.query(db.Notice).filter(db.Notice.ivorn == notice.ivorn).one()

                assert notice.type == db_notice.gcn.type
                assert notice.skymap == db_notice.gcn.skymap

            print('Passed!')
