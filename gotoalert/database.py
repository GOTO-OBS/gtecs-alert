#! /opt/local/bin/python3.6
"""Functions to add events into the GOTO Observation Database."""

import astropy.units as u

from gototile.grid import SkyGrid

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
                     'startUTC': None,
                     'stopUTC': None,
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

GW_MPOINTING = {'userKey': None,
                'objectName': None,
                'ra': None,
                'decl': None,
                # auto filled values
                'minTime': None,
                'startUTC': None,
                'stopUTC': None,
                # put in at rank 6, marked as ToO
                'ToO': True,
                'start_rank': 1,
                # default to 3 pointings, at least hour apart, valid for a day
                'num_todo': 99,
                'valid_time': -1,
                'wait_time': -1,
                # default values
                'maxSunAlt': -12,
                'maxMoon': 'B',
                'minMoonSep': 10,
                'minAlt': 30,
                }

GW_EXPSET = {'numexp': 3,
             'expTime': 60,
             'filt': 'L',
             'binning': 1,
             'typeFlag': 'SCIENCE',
             }


def add_single_pointing(event, log):
    """Simply add a single pointing at the coordinates given in the alert."""
    with db.open_session() as session:
        try:
            userkey = db.get_userkey(session, DEFAULT_USER)
        except Exception:
            db.add_user(session, DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)
            userkey = db.get_userkey(session, DEFAULT_USER)

        # Create Event and add it to the database
        db_event = db.Event(ivo=event.ivorn,
                            name=event.name,
                            source=event.source,
                            )
        try:
            session.add(db_event)
            session.commit()
        except Exception as err:
            session.rollback()
            raise

        # Get default Mpointing infomation and add event name and coords
        mp_data = DEFAULT_MPOINTING.copy()
        mp_data['userKey'] = userkey
        mp_data['objectName'] = event.name
        mp_data['ra'] = event.coord.ra.value
        mp_data['decl'] = event.coord.dec.value

        # Time to start immedietly after the event, expire after 4 days if not completed
        mp_data['startUTC'] = event.time
        mp_data['stopUTC'] = event.time + 4 * u.day

        # Create Mpointing
        db_mpointing = db.Mpointing(**mp_data)
        db_mpointing.event = db_event

        # Get default Exposure Set infomation
        expsets_data = [DEFAULT_EXPSET.copy()]

        # Create Exposure Sets
        for expset_data in expsets_data:
            db_exposure_set = db.ExposureSet(**expset_data)
            db_mpointing.exposure_sets.append(db_exposure_set)

        # Update mintime
        total_exptime = sum([(es['expTime'] + 30) * es['numexp'] for es in expsets_data])
        db_mpointing.minTime = total_exptime

        # Add Mpointing to the database
        try:
            session.add(db_mpointing)
            session.commit()
            log.debug(db_mpointing)
        except Exception as err:
            session.rollback()
            raise


def add_tiles(event, grid, log):
    """Use GOTO-tile to add pointings based on the alert."""
    with db.open_session() as session:
        try:
            userkey = db.get_userkey(session, DEFAULT_USER)
        except Exception:
            db.add_user(session, DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)
            userkey = db.get_userkey(session, DEFAULT_USER)

        # Create Event and add it to the database
        db_event = db.Event(ivo=event.ivorn,
                            name=event.name,
                            source=event.source,
                            )
        try:
            session.add(db_event)
            session.commit()
        except Exception as err:
            session.rollback()
            raise

        # Get the Event skymap and apply it to the grid
        skymap = event.get_skymap()
        grid.apply_skymap(skymap)

        # Get the table of tiles and contained probability
        table = grid.get_table()
        table.sort('prob')
        table.reverse()

        # Mask the table based on tile probs
        # Here we select up to the 20 most prbable tiles with contained prob > 1%
        # TODO: Different selection options: by prob, by number etc
        mask = table['prob'] > 0.01
        masked_table = table[mask][:50]

        # Create Mpointings for each tile
        mpointings = []
        for tilename, ra, dec, prob in masked_table:

            # Create an EventTile
            # TODO: Replace surveys and events with grids and surveys
            #       See https://github.com/GOTO-OBS/goto-obsdb/issues/16
            db_tile = db.EventTile(ra=ra.deg, decl=dec.deg,
                                   probability=float(prob),
                                   unobserved_probability=float(prob)  # if trigger fails
                                   )
            db_tile.event = db_event

            # Get default Mpointing infomation and add event name and coords
            if event.type == 'GW':
                mp_data = GW_MPOINTING.copy()
            else:
                mp_data = DEFAULT_MPOINTING.copy()
            mp_data['userKey'] = userkey
            mp_data['objectName'] = event.name + '_' + tilename
            mp_data['ra'] = ra.deg
            mp_data['decl'] = dec.deg

            # Time to start immedietly after the event, expire after X days if not completed
            mp_data['startUTC'] = event.time
            if event.type == 'GW':
                mp_data['stopUTC'] = None
            else:
                mp_data['stopUTC'] = event.time + 4 * u.day

            # Create Mpointing
            db_mpointing = db.Mpointing(**mp_data)
            db_mpointing.event = db_event
            db_mpointing.eventTile = db_tile

            # Get default Exposure Set infomation
            if event.type == 'GW':
                expsets_data = [GW_EXPSET.copy()]
            else:
                expsets_data = [DEFAULT_EXPSET.copy()]

            # Create Exposure Sets
            for expset_data in expsets_data:
                db_exposure_set = db.ExposureSet(**expset_data)
                db_mpointing.exposure_sets.append(db_exposure_set)

            # Update mintime
            total_exptime = sum([(es['expTime'] + 30) * es['numexp'] for es in expsets_data])
            db_mpointing.minTime = total_exptime

            # Add to list
            mpointings.append(db_mpointing)

        # Add Mpointings to the database
        try:
            db.insert_items(session, mpointings)
            session.commit()
            log.debug('Added {} mpointings'.format(len(mpointings)))
        except Exception as err:
            session.rollback()
            raise


def db_insert(event, log, on_grid=True):
    """Insert an event into the ObsDB.

    If on_grid is True given then work out which tiles the event covers using GOTO-tile.
    If not then just add a single pointing at the event centre.

    In the future this will need to alter parameters depending on the event,
    most obviously the rank but also different filter sets, times between visits and so on.
    """
    log.info('Inserting event {} into GOTO database'.format(event.name))

    try:
        if not on_grid:
            # Add a single pointing at the event centre
            add_single_pointing(event, log)
        else:
            # Add a series of on-grid pointings based on a Gaussian skymap
            # TODO: We should load the grid from the database, but it's not there yet
            #       For now it's hardcoded here
            fov = {'ra': 5.5 * u.deg, 'dec': 2.6 * u.deg}
            overlap = {'ra': 0.1, 'dec': 0.1}
            grid = SkyGrid(fov, overlap)

            add_tiles(event, grid, log)
        log.info('Database insersion complete')

    except Exception as err:
        log.error(err)
        log.warning('Unable to insert event into database')
