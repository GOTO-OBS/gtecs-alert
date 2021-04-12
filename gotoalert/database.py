#! /opt/local/bin/python3.6
"""Functions to add events into the GOTO Observation Database."""

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


def get_mpointing_info(event):
    """Get all the infomation needed for database Mpointings and ExposureSets.

    Parameters will vary depending on the type of Event and the chosen strategy.
    """
    # Fill out ExposureSet info
    # (do this first as it's needed for the mintime)
    expsets = []
    for expset_dict in event.strategy['exposure_sets_dict']:
        exp_data = {}
        exp_data['num_exp'] = int(expset_dict['num_exp'])
        exp_data['exptime'] = float(expset_dict['exptime'])
        exp_data['filt'] = str(expset_dict['filt'])
        expsets.append(exp_data)

    # To handle multiple cadence strategies we need to create multiple sets of Mpointings
    if isinstance(event.strategy['cadence_dict'], dict):
        cadence_dicts = [event.strategy['cadence_dict']]
    else:
        cadence_dicts = event.strategy['cadence_dict']

    mp_list = []
    for cadence_dict in cadence_dicts:
        # Create the blank Mpointing data dict
        mp_data = {}

        # All Events should be Targets of Opportunity, that's the point!
        mp_data['too'] = True

        # The minimum pointing time is based on the ExposureSet +30s for readout, probably generous
        mp_data['min_time'] = sum((exp_data['exptime'] + 30) * exp_data['num_exp']
                                  for exp_data in expsets)

        # Rank is the same for all cadences
        mp_data['start_rank'] = int(event.strategy['rank'])

        # Fill out cadence details
        mp_data['start_time'] = str(cadence_dict['start_time'])
        mp_data['stop_time'] = str(cadence_dict['stop_time'])
        mp_data['num_todo'] = int(cadence_dict['num_todo'])
        mp_data['wait_time'] = cadence_dict['wait_time']  # Can be a list of floats

        # Constraints are the same for all cadences
        constraints_dict = event.strategy['constraints_dict']
        mp_data['max_sunalt'] = float(constraints_dict['max_sunalt'])
        mp_data['min_alt'] = float(constraints_dict['min_alt'])
        mp_data['min_moonsep'] = float(constraints_dict['min_moonsep'])
        mp_data['max_moon'] = str(constraints_dict['max_moon'])

        mp_list.append(mp_data)

    return mp_list, expsets


def get_user(session):
    """Get the database user, or create one if it doesn't exist."""
    try:
        user = db.get_user(session, username=DEFAULT_USER)
    except ValueError:
        user = db.User(DEFAULT_USER, DEFAULT_PW, DEFAULT_NAME)
    return user


def get_grid_tiles(event, db_grid):
    """Apply the Event skymap to the current grid and return a table of filtered tiles."""
    # Create a SkyGrid from the database Grid
    grid = db_grid.get_skygrid()
    event.grid = grid

    # Apply the Event skymap to the grid
    if not event.skymap:
        event.get_skymap()
    grid.apply_skymap(event.skymap)

    # Chose the contour level.
    # NOTE: The code below is rather preliminary, based of what's best for 4- or 8-UT systems.
    # It needs simulating to find the optimal value.
    if grid.tile_area < 20:
        # GOTO-4
        contour_level = 0.9
    else:
        # GOTO-8
        contour_level = 0.95

    # Get the table of tiles selected depending on the event
    masked_table = grid.select_tiles(contour=contour_level,
                                     max_tiles=event.strategy['tile_limit'],
                                     min_tile_prob=event.strategy['prob_limit'],
                                     )

    # Sort the tables and store on the Event
    masked_table.sort('prob')
    masked_table.reverse()
    event.masked_table = masked_table

    # Also sort and store the full table
    full_table = grid.get_table()
    full_table.sort('prob')
    full_table.reverse()
    event.full_table = full_table

    return masked_table


def add_to_database(event, log):
    """Add the Event into the database."""
    with db.open_session() as session:
        # Create Event
        db_event = db.Event(name=event.name,
                            ivorn=event.ivorn,
                            source=event.source,
                            event_type=event.type,
                            time=event.time,
                            skymap=event.skymap_url if hasattr(event, 'skymap_url') else None,
                            )
        log.debug('Adding Event to database')
        session.add(db_event)

        # If it's a retraction event that's all we need to do
        if event.type == 'GW_RETRACTION':
            return

        if event.strategy['on_grid']:
            # Get the current grid
            db_grid = db.get_current_grid(session)
            log.info('Applying to Grid {}'.format(db_grid.name))

            # Get the masked tile table
            tile_table = get_grid_tiles(event, db_grid)
            log.debug('Masked tile table has {} entries'.format(len(tile_table)))

            # We might have excluded all of our tiles, if so exit
            if len(tile_table) < 1:
                log.warning('No tiles passed filtering, no pointings to add')
                log.debug('Highest tile has {:.2f}%'.format(max(event.full_table['prob']) * 100))
                return

            # Create Survey
            db_survey = db.Survey(name=event.name)
            db_survey.grid = db_grid
            db_survey.event = db_event
            log.debug('Adding Survey to database')
            session.add(db_survey)

        # Get the database User, or make it if it doesn't exist
        db_user = get_user(session)

        # Get Mpointing and ExposureSet infomation
        mp_list, expsets = get_mpointing_info(event)

        # Create Mpointing(s)
        mpointings = []
        for mp_data in mp_list:
            if not event.strategy['on_grid']:
                # Create a single Mpointing
                db_mpointing = db.Mpointing(object_name=event.name,
                                            ra=event.coord.ra.value,
                                            dec=event.coord.dec.value,
                                            **mp_data)
                db_mpointing.user = db_user
                db_mpointing.event = db_event

                # Add to list
                mpointings.append(db_mpointing)
            else:
                # Create Mpointings for each tile
                for tilename, _, _, weight in tile_table:
                    # Find the matching GridTile
                    query = session.query(db.GridTile)
                    query = query.filter(db.GridTile.grid == db_grid,
                                         db.GridTile.name == tilename)
                    db_grid_tile = query.one_or_none()

                    # Create a SurveyTile
                    db_survey_tile = db.SurveyTile(weight=float(weight))
                    db_survey_tile.survey = db_survey
                    db_survey_tile.grid_tile = db_grid_tile

                    # Create Mpointing
                    db_mpointing = db.Mpointing(object_name='{}_{}'.format(event.name, tilename),
                                                ra=None,
                                                dec=None,
                                                **mp_data)
                    db_mpointing.user = db_user
                    db_mpointing.grid_tile = db_grid_tile
                    db_mpointing.survey_tile = db_survey_tile
                    db_mpointing.event = db_event

                    # Add to list
                    mpointings.append(db_mpointing)

        for db_mpointing in mpointings:
            # Create Exposure Sets
            for exp_data in expsets:
                db_exposure_set = db.ExposureSet(**exp_data)
                db_mpointing.exposure_sets.append(db_exposure_set)

            # Create the first Pointing (i.e. preempt the caretaker)
            db_pointing = db_mpointing.get_next_pointing()
            db_mpointing.pointings.append(db_pointing)

        # Add Mpointings to the database
        log.debug('Adding {} Mpointings to database'.format(len(mpointings)))
        db.insert_items(session, mpointings)

        # Commit changes
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise
