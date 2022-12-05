"""Functions to add events into the GOTO Observation Database."""

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as db


def get_user(session):
    """Get the sentinel database user, or create one if it doesn't exist."""
    try:
        user = db.get_user(session, username='sentinel')
    except ValueError:
        user = db.User('sentinel', '', 'Sentinel Alert Listener')
    return user


def get_event(session, event):
    """Get the database event entry, or create one if it doesn't exist."""
    db_event = session.query(db.Event).filter(db.Event.name == event.name).one_or_none()
    if db_event is None:
        db_event = db.Event(
            name=event.name,
            source=event.source,
            type=event.type,
            time=event.time,
        )
    return db_event


def get_grid_tiles(event, grid):
    """Apply the Event skymap to the current grid and return a table of filtered tiles."""
    # Apply the Event skymap to the grid
    event.grid = grid
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


def add_to_database(event, log, time=None):
    """Add the Event into the database."""
    if time is None:
        time = Time.now()

    with db.open_session() as session:
        # Get the database Event (or make one if it's new)
        db_event = get_event(session, event)
        session.add(db_event)  # Need to add now, so it has an ID for the query below
        session.commit()

        # Go through any previous Surveys for this Event and "delete" any incomplete Targets
        # Using target.mark_deleted() will also delete any pending Pointings for each Target,
        # but won't interrupt one if it's currently running.
        log.debug('Event {} has {} previous Surveys in the database'.format(event.name,
                                                                            len(db_event.surveys)))
        for db_survey in db_event.surveys:
            # TODO: This could be a lot smarter.
            #       We don't want to delete Pointings if the skymap hasn't changed?
            #       But we might still want to adjust the strategy...
            #       For now we always reset the database on every update.
            num_deleted = 0
            for db_target in db_survey.targets:
                if db_target.status_at_time(time) not in ['deleted', 'expired', 'completed']:
                    db_target.mark_deleted(time=time)
                    num_deleted += 1
            if num_deleted > 0:
                log.debug('Deleted {} Targets for Survey {}'.format(num_deleted, db_survey.name))
            session.commit()

        # If it's a retraction event that's all we need to do
        if event.strategy is None:
            return

        # Create the new Survey
        db_survey = db.Survey(name=f'{event.name}_{len(db_event.surveys) + 1}',
                              skymap=event.skymap_url,  # TODO: What about Gaussian/file skymaps?
                              )
        db_survey.event = db_event
        log.debug('Adding Survey {} to database'.format(db_survey.name))
        session.add(db_survey)  # Add now, even if we don't add any tiles below
        session.commit()

        # Get the grid tiles from the skymap
        db_grid = db.get_current_grid(session)
        log.info('Applying to Grid {}'.format(db_grid.name))
        tile_table = get_grid_tiles(event, db_grid.skygrid)
        log.debug('Masked tile table has {} entries'.format(len(tile_table)))
        if len(tile_table) < 1:
            log.warning('No tiles passed filtering, nothing to add to the database')
            log.debug('Highest tile has {:.2f}%'.format(max(event.full_table['prob']) * 100))
            return

        # Get the database User, or make it if it doesn't exist
        db_user = get_user(session)

        # Create Targets for each tile
        db_targets = []
        for tile_name, _, _, tile_weight in tile_table:
            # Find the matching GridTile
            query = session.query(db.GridTile)
            query = query.filter(db.GridTile.grid == db_grid)
            query = query.filter(db.GridTile.name == str(tile_name))
            db_grid_tile = query.one_or_none()

            # Create ExposureSets
            db_exposure_sets = []
            for exposure_sets_dict in event.strategy['exposure_sets_dict']:
                db_exposure_sets.append(
                    db.ExposureSet(
                        num_exp=exposure_sets_dict['num_exp'],
                        exptime=exposure_sets_dict['exptime'],
                        filt=exposure_sets_dict['filt'],
                    )
                )

            # Create Strategies
            constraints_dict = event.strategy['constraints_dict']
            if isinstance(event.strategy['cadence_dict'], dict):
                cadence_dicts = [event.strategy['cadence_dict']]
            else:
                cadence_dicts = event.strategy['cadence_dict']
            db_strategies = []
            for cadence_dict in cadence_dicts:
                db_strategies.append(
                    db.Strategy(
                        num_todo=cadence_dict['num_todo'],
                        stop_time=cadence_dict['stop_time'],
                        wait_time=cadence_dict['wait_hours'] * u.hour,
                        valid_time=cadence_dict['valid_days'] * u.day,
                        min_time=None,
                        too=True,
                        min_alt=constraints_dict['min_alt'],
                        max_sunalt=constraints_dict['max_sunalt'],
                        max_moon=constraints_dict['max_moon'],
                        min_moonsep=constraints_dict['min_moonsep'],
                        # TODO: tel_mask?
                    )
                )

            # Create Target (this will automatically create Pointings)
            # NB we take the earliest start time and latest stop time from all cadences
            db_targets.append(
                db.Target(
                    name=f'{event.name}_{tile_name}',
                    ra=None,  # RA/Dec are inherited from the grid tile
                    dec=None,
                    rank=event.strategy['rank'],
                    weight=float(tile_weight),
                    start_time=cadence_dicts[0]['start_time'],
                    stop_time=cadence_dicts[-1]['stop_time'],
                    creation_time=time,
                    user=db_user,
                    grid_tile=db_grid_tile,
                    exposure_sets=db_exposure_sets,
                    strategies=db_strategies,
                    survey=db_survey,
                    event=db_event,
                )
            )

        # Add everything to the database
        log.debug('Adding {} Targets to the database'.format(len(db_targets)))
        db.insert_items(session, db_targets)

        # Commit changes
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise
