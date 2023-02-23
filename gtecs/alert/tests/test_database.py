#!/usr/bin/env python3
"""A simple test script for the GOTO-alert database."""

import importlib.resources as pkg_resources

from gtecs.alert import database as db
from gtecs.alert.events import Event


if __name__ == '__main__':
    print('~~~~~~~~~~~~~~~')
    test_files = sorted([f for f in pkg_resources.contents('gtecs.alert.data.test_events')
                         if f.endswith('.xml')])
    print('Found {} test events:'.format(len(test_files)))
    for test_file in test_files:
        print(' - ', test_file)

    for test_file in test_files:
        print('~~~~~~~~~~~~~~~')
        print('Adding to database')
        with pkg_resources.path('gtecs.alert.data.test_events', test_file) as f:
            print(f'Loading {f}')
            event = Event.from_file(f)
        event.get_skymap()
        with db.open_session() as s:
            db_voevent = db.VOEvent.from_event(event)
            s.add(db_voevent)

        print('Loading from database')
        with db.open_session() as s:
            db_voevent = s.query(db.VOEvent).filter(db.VOEvent.ivorn == event.ivorn).one()

            assert event.packet_type == db_voevent.event.packet_type
            assert event.skymap == db_voevent.event.skymap
