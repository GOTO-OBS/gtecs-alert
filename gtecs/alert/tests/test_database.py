#!/usr/bin/env python3
"""A simple test script for the GOTO-alert database."""

import importlib.resources as pkg_resources

from gtecs.alert import database as db
from gtecs.alert.notices import GCNNotice


if __name__ == '__main__':
    print('~~~~~~~~~~~~~~~')
    test_files = sorted([f for f in pkg_resources.contents('gtecs.alert.data.test_notices')
                         if f.endswith('.xml')])
    print('Found {} test notices:'.format(len(test_files)))
    for test_file in test_files:
        print(' - ', test_file)

    for test_file in test_files:
        print('~~~~~~~~~~~~~~~')
        print('Adding to database')
        with pkg_resources.path('gtecs.alert.data.test_notices', test_file) as f:
            print(f'Loading {f}')
            notice = GCNNotice.from_file(f)
        notice.get_skymap()
        with db.session_manager() as session:
            db_notice = db.Notice.from_gcn(notice)
            session.add(db_notice)

        print('Loading from database')
        with db.session_manager() as session:
            db_notice = session.query(db.Notice).filter(db.Notice.ivorn == notice.ivorn).one()

            assert notice.packet_type == db_notice.gcn.packet_type
            assert notice.skymap == db_notice.gcn.skymap
