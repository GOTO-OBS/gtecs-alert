#! /opt/local/bin/python3.6
"""Functions to add events into the GOTO Observation Database."""

import astropy.units as u

from gototile.grid import SkyGrid

import numpy as np

import obsdb as db


DEFAULT_USER = 'goto'
DEFAULT_PW = 'gotoobs'
DEFAULT_NAME = 'GOTO automated alerts'


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


def get_mpointing_info(strategy_dict):
    """Format all the infomation needed for a database Mpointing and ExposureSets.

    Parameters will vary depending on the type of Event.
    """
    # Fill out ExposureSet info
    # (do this first as it's needed for the mintime)
    expsets = []
    for expset_dict in strategy_dict['exposure_sets_dict']:
        exp_data = {}
        exp_data['num_exp'] = int(expset_dict['num_exp'])
        exp_data['exptime'] = float(expset_dict['exptime'])
        exp_data['filt'] = str(expset_dict['filt'])
        # These are always the same
        exp_data['binning'] = 1
        exp_data['imgtype'] = 'SCIENCE'
        expsets.append(exp_data)

    # Create the blank Mpointing data dict
    mp_data = {}

    # Add blank object name and coordinates
    # They will depend on if it's on grid or not
    mp_data['object_name'] = None
    mp_data['ra'] = None
    mp_data['dec'] = None

    # All Events should be Targets of Opportunity, that's the point!
    mp_data['too'] = True

    # The minimum pointing time is based on the ExposureSet +30s for readout, probably generous
    mp_data['min_time'] = np.sum((exp_data['exptime'] + 30) * exp_data['num_exp']
                                 for exp_data in expsets)

    # The valid time is always infinite, not needed for these sort of events
    mp_data['valid_time'] = -1

    # Everything else comes from the strategy_dict and it's subdicts
    mp_data['start_time'] = str(strategy_dict['start_time'])
    mp_data['start_rank'] = int(strategy_dict['rank'])

    cadence_dict = strategy_dict['cadence_dict']
    mp_data['num_todo'] = int(cadence_dict['num_todo'])
    mp_data['wait_time'] = cadence_dict['wait_time']  # Can be a list of floats
    mp_data['stop_time'] = strategy_dict['start_time'] + cadence_dict['valid_days'] * u.day

    constraints_dict = strategy_dict['constraints_dict']
    mp_data['max_sunalt'] = float(constraints_dict['max_sunalt'])
    mp_data['min_alt'] = float(constraints_dict['min_alt'])
    mp_data['min_moonsep'] = float(constraints_dict['min_moonsep'])
    mp_data['max_moon'] = str(constraints_dict['max_moon'])

    return mp_data, expsets


def add_single_pointing(event, strategy_dict, log):
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

        # Get default Mpointing and ExposureSet infomation
        mp_data, expsets = get_mpointing_info(strategy_dict)

        # Set the object name and coordinates
        mp_data['object_name'] = event.name
        mp_data['ra'] = event.coord.ra.value
        mp_data['dec'] = event.coord.dec.value

        # Create Mpointing
        db_mpointing = db.Mpointing(**mp_data, user=user)
        db_mpointing.event = db_event

        # Create Exposure Sets
        for exp_data in expsets:
            db_exposure_set = db.ExposureSet(**exp_data)
            db_mpointing.exposure_sets.append(db_exposure_set)

        # Create the first Pointing (i.e. preempt the caretaker)
        db_pointing = db_mpointing.get_next_pointing()
        db_pointing.event = db_event
        db_mpointing.pointings.append(db_pointing)

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


def add_tiles(event, strategy_dict, log):
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

        # Check the Event has its skymap
        if not event.skymap:
            log.debug('Fetching skymap')
            event.get_skymap()

        # Apply the skymap to the grid
        log.debug('Applying skymap to grid')
        grid.apply_skymap(event.skymap)

        # Store grid on the Event
        event.grid = grid

        # Get the table of tiles and contained probability
        table = grid.get_table()

        # Mask the table based on tile probs
        log.debug('Masking tile table')
        # see https://github.com/GOTO-OBS/goto-alert/issues/26
        # mask based on if the mean tile pixel value is within the 90% contour
        mask = [np.mean(event.skymap.contours[tile]) < 0.9 for tile in grid.pixels]
        if sum(mask) < 1:
            # The source is probably so well localised that no tile has a mean contour of < 90%
            # This can happen for Swift GRBs.
            # Instead just mask to any tiles with a contained probability of > 90%
            # Probably just one, unless it's in an overlap region
            mask = table['prob'] > 0.9
        masked_table = table[mask]

        # Limit the number of tiles added
        masked_table.sort('prob')
        masked_table.reverse()
        masked_table = masked_table[:strategy_dict['tile_limit']]

        # Store table on the Event
        log.debug('Masked tile table has {} entries'.format(len(masked_table)))
        event.tile_table = masked_table
        event.full_table = table

        # We might have excluded all of our tiles, if so exit
        if not len(masked_table):
            log.warning('No tiles passed filtering, no pointings to add')
            log.debug('Highest tile has {:.3f}%'.format(table[0]['prob'] * 100))
            return

        # Create Event and add it to the database
        db_event = db.Event(name=event.name,
                            ivorn=event.ivorn,
                            source=event.source,
                            event_type=event.type,
                            time=event.time,
                            skymap=event.skymap_url if hasattr(event, 'skymap_url') else None,
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

            # Get default Mpointing and ExposureSet infomation
            mp_data, expsets = get_mpointing_info(strategy_dict)

            # Set the object name, combination of event name and tile name
            mp_data['object_name'] = event.name + '_' + tilename

            # Create Mpointing
            db_mpointing = db.Mpointing(**mp_data, user=user)
            db_mpointing.grid_tile = db_grid_tile
            db_mpointing.survey_tile = db_survey_tile
            db_mpointing.event = db_event

            # Create Exposure Sets
            for exp_data in expsets:
                db_exposure_set = db.ExposureSet(**exp_data)
                db_mpointing.exposure_sets.append(db_exposure_set)

            # Create the first Pointing (i.e. preempt the caretaker)
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
