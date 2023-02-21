#!/usr/bin/env python3
"""A simple test script for the GOTO-alert database."""

import importlib.resources as pkg_resources
from gzip import GzipFile
from io import BytesIO

from astropy.io import fits

from gototile.skymap import SkyMap

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
        event.add_to_database()

        print('Loading from database')
        with db.open_session() as s:
            db_voevent = s.query(db.VOEvent).filter(db.VOEvent.ivorn == event.ivorn).one()

            event_new = Event.from_payload(db_voevent.payload)
            assert event.packet_type == event_new.packet_type

            if db_voevent.skymap is not None:
                try:
                    hdu = fits.open(BytesIO(db_voevent.skymap))
                except OSError:
                    # It might be compressed
                    gzip = GzipFile(fileobj=BytesIO(db_voevent.skymap), mode='rb')
                    hdu = fits.open(gzip)
                skymap_new = SkyMap.from_fits(hdu)
                assert event.skymap == skymap_new
