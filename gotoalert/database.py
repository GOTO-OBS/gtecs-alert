#! /opt/local/bin/python3.6
"""Functions to add events into the GOTO Observation Database."""

import obsdb as db

DEFAULT_USER = 'goto'
DEFAULT_PW = 'gotoobs'
DEFAULT_NAME = 'GOTO automated alerts'

DEFAULT_MPOINTING = {'userKey': None,
                     'objectName': None,
                     'ra': None,
                     'decl': None,
                     # auto filled values
                     'minTime': None,
                     # put in at rank 6, marked as ToO
                     'ToO': True,
                     'start_rank': 6,
                     # default to 3 pointings, at least hour apart, valid for a day
                     'num_todo': 3,
                     'valid_time': 24 * 60,
                     'wait_time': 60,
                     # default values
                     'maxSunAlt': -15,
                     'maxMoon': 'B',
                     'minMoonSep': 30,
                     'minAlt': 40,
                     }

DEFAULT_EXPSET = {'numexp': 5,
                  'expTime': 120,
                  'filt': 'L',
                  'binning': 1,
                  'typeFlag': 'SCIENCE',
                  }


def db_insert(event_data, log):
    """Insert an event into the ObsDB.

    In the future this will need to work out which tiles the event covers, using GOTO-tile to
    parse skymaps and so on. Even for a single pointing it should be inserted on-grid.
    It should also alter the parameters depending on the event, most obviously the rank but also
    different filter sets, times between visits and so on.

    But for now just add a single pointing at the event centre, with pre-set parameters.
    """
    log.info('Inserting event {} into GOTO database'.format(event_data['event_name']))

    # Add a single pointing
    try:
        add_single_pointing(event_data, log)
        log.info('Database insersion complete')
    except Exception as err:
        log.error(err)
        log.warning('Unable to insert event into database')


def add_single_pointing(event_data, log):
    """Simply add a single pointing at the coordinates given in the alert."""
    with db.open_session() as session:
        try:
            userkey = db.get_userkey(session, DEFAULT_USER)
        except Exception:
            db.add_user(session, DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)
            userkey = db.get_userkey(session, DEFAULT_USER)

        # Create Event and add it to the database
        event = db.Event(ivo=event_data['ivorn'],
                         name=event_data['event_name'],
                         source=event_data['source'],
                         )
        try:
            session.add(event)
            session.commit()
        except Exception as err:
            session.rollback()
            raise

        # Get default Mpointing infomation and add event name and coords
        mp_data = DEFAULT_MPOINTING.copy()
        mp_data['userKey'] = userkey
        mp_data['objectName'] = event_data['event_name']
        coord = event_data['event_coord']
        mp_data['ra'] = coord.ra.value
        mp_data['decl'] = coord.dec.value

        # Create Mpointing
        mpointing = db.Mpointing(**mp_data)
        mpointing.event = event

        # Get default Exposure Set infomation
        expsets_data = [DEFAULT_EXPSET.copy()]

        # Create Exposure Sets
        for expset_data in expsets_data:
            exposure_set = db.ExposureSet(**expset_data)
            mpointing.exposure_sets.append(exposure_set)

        # Update mintime
        total_exptime = sum([(es['expTime'] + 30) * es['numexp'] for es in expsets_data])
        mpointing.minTime = total_exptime

        # Add Mpointing to the database
        try:
            session.add(mpointing)
            session.commit()
            log.debug(mpointing)
        except Exception as err:
            session.rollback()
            raise
