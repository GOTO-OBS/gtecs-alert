#! /opt/local/bin/python3.6
"""Functions to add events into the GOTO Observation Database."""

import astropy.units as u

from gototile.grid import SkyGrid

import obsdb as db

from . import params


DEFAULT_USER = 'goto'
DEFAULT_PW = 'gotoobs'
DEFAULT_NAME = 'GOTO automated alerts'

DEFAULT_MPOINTING = {'object_name': None,
                     'ra': None,
                     'dec': None,
                     # auto filled values
                     'min_time': None,
                     'start_time': None,
                     'stop_time': None,
                     # put in at rank 6, marked as ToO
                     'too': True,
                     'start_rank': 106,
                     # default to 3 pointings, at least hour apart, valid for a day
                     'num_todo': 3,
                     'valid_time': 24 * 60,
                     'wait_time': 60,
                     # default values
                     'max_sunalt': -15,
                     'max_moon': 'B',
                     'min_moonsep': 30,
                     'min_alt': 30,
                     }

DEFAULT_EXPSET = {'num_exp': 3,
                  'exptime': 60,
                  'filt': 'L',
                  'binning': 1,
                  'imgtype': 'SCIENCE',
                  }

GW_MPOINTING = {'object_name': None,
                'ra': None,
                'dec': None,
                # auto filled values
                'min_time': None,
                'start_time': None,
                'stop_time': None,
                # put in at rank 6, marked as ToO
                'too': True,
                'start_rank': 1,
                # default to 3 pointings, at least hour apart, valid for a day
                'num_todo': 99,
                'valid_time': -1,
                'wait_time': -1,
                # default values
                'max_sunalt': -12,
                'max_moon': 'B',
                'min_moonsep': 10,
                'min_alt': 30,
                }

GW_EXPSET = {'num_exp': 3,
             'exptime': 60,
             'filt': 'L',
             'binning': 1,
             'imgtype': 'SCIENCE',
             }


def remove_previous_events(event, log):
    """Check the database Events table to see if there's a previous instance of the event.

    If any are found then any pending pointings and mpointings will be removed from the queue
    (status set to 'deleted' in the database, not actually dropped).
    """
    with db.open_session() as session:
        # Check the events table for any previous entries of the same event
        query = session.query(db.Event).filter(db.Event.name == event.name,
                                               db.Event.source == event.source)
        db_events = query.all()

        if not db_events:
            # Nothing to worry about, it's a new event
            log.info('Event {} has no previous entry in the database'.format(event.name))
            return

        if any([db_event.ivorn == event.ivorn for db_event in db_events]):
            # Something's wrong, IVORN should be a unique column so we can't add this one
            raise ValueError('ivorn={} already exists in the database'.format(event.ivorn))

        # So there is (at least one) previous entry for this event
        log.debug('Event {} has {} previous entries in the database'.format(event.name,
                                                                            len(db_events)))
        for db_event in db_events:
            # Get any Mpointings for this event
            # Note both scheduled and unscheduled, but we don't care about completed or expired
            # or already deleted (if this is the 2nd+ update)
            query = session.query(db.Mpointing).filter(db.Mpointing.event == db_event,
                                                       db.Mpointing.status.in_(('scheduled',
                                                                                'unscheduled')))
            db_mpointings = query.all()

            # Delete the Mpointings
            for db_mpointing in db_mpointings:
                db_mpointing.status = 'deleted'

            # Get any pending pointings related to this event
            # Note only pending, if one's running we don't want to delete it and we don't care
            # about finished ones (completed, aborted, interrupted) or expired
            # or already deleted (if this is the 2nd+ update)
            query = session.query(db.Pointing).filter(db.Pointing.event == db_event,
                                                      db.Pointing.status == 'pending')
            db_pointings = query.all()

            # Delete the Pointings
            for db_pointing in db_pointings:
                db_pointing.status = 'deleted'

            # Commit changes
            session.commit()

            if len(db_mpointings) > 0 or len(db_pointings) > 0:
                log.info('Deleted {} Mpointings and {} Pointings from previous Event {}'.format(
                         len(db_mpointings), len(db_pointings), db_event.ivorn))


def add_single_pointing(event, log):
    """Simply add a single pointing at the coordinates given in the alert."""
    with db.open_session() as session:
        # Get the User, or make it if it doesn't exist
        try:
            user = db.get_user(session, username=DEFAULT_USER)
        except ValueError:
            user = db.User(DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)

        # Create Event and add it to the database
        db_event = db.Event(ivorn=event.ivorn,
                            name=event.name,
                            source=event.source,
                            )
        log.debug('Adding Event to database')
        try:
            session.add(db_event)
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise

        # Get default Mpointing infomation and add event name and coords
        mp_data = DEFAULT_MPOINTING.copy()
        mp_data['object_name'] = event.name
        mp_data['ra'] = event.coord.ra.value
        mp_data['dec'] = event.coord.dec.value

        # Time to start immedietly after the event, expire after 4 days if not completed
        mp_data['start_time'] = event.time
        mp_data['stop_time'] = event.time + 4 * u.day

        # Create Mpointing
        db_mpointing = db.Mpointing(**mp_data, user=user)
        db_mpointing.event = db_event

        # Get default Exposure Set infomation
        expsets_data = [DEFAULT_EXPSET.copy()]

        # Create Exposure Sets
        for expset_data in expsets_data:
            db_exposure_set = db.ExposureSet(**expset_data)
            db_mpointing.exposure_sets.append(db_exposure_set)

        # Update mintime
        total_exptime = sum([(es['exptime'] + 30) * es['num_exp'] for es in expsets_data])
        db_mpointing.min_time = total_exptime

        # Add Mpointing to the database
        log.debug('Adding Mpointing to database')
        try:
            session.add(db_mpointing)
            session.commit()
            log.debug(db_mpointing)
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise


def add_tiles(event, log):
    """Use GOTO-tile to add pointings based on the alert."""
    with db.open_session() as session:
        # Get the User, or make it if it doesn't exist
        try:
            user = db.get_user(session, username=DEFAULT_USER)
        except ValueError:
            user = db.User(DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)

        # Find the current Grid in the database
        db_grids = session.query(db.Grid).all()
        if not db_grids:
            raise ValueError('No defined Grids found!')
        else:
            # Might have multiple grids defined, just take the latest...
            db_grid = db_grids[-1]
            log.info('Applying to Grid {}'.format(db_grid.name))

        # Create a SkyGrid from the database Grid
        fov = {'ra': db_grid.ra_fov * u.deg, 'dec': db_grid.dec_fov * u.deg}
        overlap = {'ra': db_grid.ra_overlap, 'dec': db_grid.dec_overlap}
        grid = SkyGrid(fov, overlap, kind=db_grid.algorithm)

        # Get the Event skymap and apply it to the grid
        log.debug('Fetching skymap')
        skymap = event.get_skymap()
        log.debug('Applying skymap to grid')
        grid.apply_skymap(skymap)

        # Store grid on the Event
        event.grid = grid

        # Get the table of tiles and contained probability
        table = grid.get_table()
        table.sort('prob')
        table.reverse()

        # Mask the table based on tile probs
        log.debug('Masking tile table')
        if event.type == 'GW':
            mask = table['prob'] > 0.01
        elif params.MIN_TILE_PROB:
            mask = table['prob'] > params.MIN_TILE_PROB
        else:
            # Still remove super-low probability tiles (0.01%)
            mask = table['prob'] > 0.0001
        masked_table = table[mask]

        # Limit the number of tiles added
        if event.type == 'GW':
            masked_table = masked_table[:50]
        elif params.MAX_TILES:
            masked_table = masked_table[:params.MAX_TILES]

        # Store table on the Event
        log.debug('Masked tile table has {} entries'.format(len(masked_table)))
        event.tile_table = masked_table
        event.full_table = table

        # We might have excluded all of our tiles, if so exit
        if not len(masked_table):
            log.warning('No tiles passed filtering, no pointings to add')
            return

        # Create Event and add it to the database
        db_event = db.Event(name=event.name,
                            ivorn=event.ivorn,
                            source=event.source,
                            event_type=event.type,
                            time=event.time,
                            skymap=event.skymap_url,
                            )
        log.debug('Adding Event to database')
        try:
            session.add(db_event)
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise

        # Create Survey and add it to the database
        db_survey = db.Survey(name=event.name)
        db_survey.grid = db_grid
        db_survey.event = db_event
        log.debug('Adding Survey to database')
        session.add(db_survey)

        # Create Mpointings for each tile
        # NB no coords, we get them from the GridTile
        mpointings = []
        for tilename, _, _, prob in masked_table:
            # Find the matching GridTile
            query = session.query(db.GridTile)
            query = query.filter(db.GridTile.grid == db_grid,
                                 db.GridTile.name == tilename)
            db_grid_tile = query.one_or_none()

            # Create a SurveyTile
            db_survey_tile = db.SurveyTile(weight=float(prob))
            db_survey_tile.survey = db_survey
            db_survey_tile.grid_tile = db_grid_tile

            # Get default Mpointing infomation and add event name
            if event.type == 'GW':
                mp_data = GW_MPOINTING.copy()
            else:
                mp_data = DEFAULT_MPOINTING.copy()
            mp_data['object_name'] = event.name + '_' + tilename

            # Time to start immedietly after the event, expire after X days if not completed
            mp_data['start_time'] = event.time
            if event.type == 'GW':
                mp_data['stop_time'] = None
            else:
                mp_data['stop_time'] = event.time + params.VALID_DAYS * u.day

            # Create Mpointing
            db_mpointing = db.Mpointing(**mp_data, user=user)
            db_mpointing.grid_tile = db_grid_tile
            db_mpointing.survey_tile = db_survey_tile
            db_mpointing.event = db_event

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
            total_exptime = sum([(es['exptime'] + 30) * es['num_exp'] for es in expsets_data])
            db_mpointing.min_time = total_exptime

            # Create the first pointing (i.e. preempt the caretaker)
            db_pointing = db_mpointing.get_next_pointing()

            # Attach the tiles, because get_next_pointing uses IDs but they don't have them yet!
            db_pointing.grid_tile = db_grid_tile
            db_pointing.survey_tile = db_survey_tile
            db_pointing.event = db_event

            db_mpointing.pointings.append(db_pointing)

            # Add to list
            mpointings.append(db_mpointing)

        # Add Mpointings to the database
        log.debug('Adding Mpointings to database')
        try:
            db.insert_items(session, mpointings)
            session.commit()
            log.info('Added {} Mpointings to database'.format(len(mpointings)))
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise


def db_insert(event, log, delete_old=True, on_grid=True):
    """Insert an event into the ObsDB.

    If delete_old is True thenremove any existing (m)pointings assosiated with an event of the
    same name.

    If on_grid is True then work out which tiles the event covers using GOTO-tile.
    If not then just add a single pointing at the event centre.
    """
    log.info('Inserting event {} into GOTO database'.format(event.name))

    try:
        # First we need to see if there's a previous instance of the same event already in the db
        # If so, then delete any still pending pointings and mpointings assosiated with the event
        if delete_old:
            log.debug('Checking for previous events in database')
            remove_previous_events(event, log)

        # Then add the new pointings
        if not on_grid:
            # Add a single pointing at the event centre
            log.debug('Adding a single pointing to database')
            add_single_pointing(event, log)
        else:
            # Add a series of on-grid pointings based on a Gaussian skymap
            # We load the latest all-sky grid from the database
            log.debug('Adding on-grid pointings to database')
            add_tiles(event, log)
        log.info('Database insersion complete')

    except Exception:
        log.warning('Unable to insert event into database')
        raise
