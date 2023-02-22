"""Functions for handling VOEvents."""

import logging

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as db

from .slack import send_database_report, send_event_report, send_slack_msg, send_strategy_report


def get_event_tiles(event, grid, selection_contour=None):
    """Apply the Event skymap to the given grid and return a table of filtered tiles."""
    # Apply the Event skymap to the grid
    skymap = event.get_skymap()
    grid.apply_skymap(skymap)

    # Sort and store the full table of tiles on the Event
    tiles = grid.get_table()

    # If no selection or strategy then just return the full table
    if event.strategy is None or selection_contour is None:
        return tiles

    # Limit tiles to add to the database
    # First select only tiles covering the given contour level
    mask = grid.contours < selection_contour

    # Then limit the number of tiles, if given
    if (event.strategy_dict['tile_limit'] is not None and
            sum(mask) > event.strategy_dict['tile_limit']):
        # Limit by probability above `tile_limit`th tile
        min_tile_prob = sorted(grid.probs, reverse=True)[event.strategy_dict['tile_limit']]
        mask &= grid.probs > min_tile_prob

    # Finally limit to tiles which contain more than a given probability
    if event.strategy_dict['prob_limit'] is not None:
        mask &= grid.probs > event.strategy_dict['prob_limit']

    # Return the masked table
    return tiles[mask]


def add_event_to_obsdb(event, time=None, log=None):
    """Add the Event into the database."""
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    with db.open_session() as session:
        # Get the database Event (or make one if it's new)
        db_event = session.query(db.Event).filter(db.Event.name == event.name).one_or_none()
        if db_event is None:
            db_event = db.Event(
                name=event.name,
                source=event.source,
                type=event.type,
                time=event.time,
            )
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
                              )
        db_survey.event = db_event
        log.debug('Adding Survey {} to database'.format(db_survey.name))
        session.add(db_survey)  # Add now, even if we don't add any tiles below
        session.commit()

        # Get the grid tiles from the skymap
        db_grid = db.get_current_grid(session)
        log.info('Applying to Grid {}'.format(db_grid.name))
        grid = db_grid.skygrid

        # Chose the contour level
        # NOTE: The code below is rather preliminary, based of what's best for 4- or 8-UT systems.
        # It needs simulating to find the optimal value.
        if grid.tile_area < 20:
            # GOTO-4
            contour_level = 0.9
        else:
            # GOTO-8
            contour_level = 0.95

        # Get the masked tile table
        selected_tiles = get_event_tiles(event, grid, contour_level)
        selected_tiles.sort('prob')
        selected_tiles.reverse()
        log.debug('Masked tile table has {} entries'.format(len(selected_tiles)))
        if len(selected_tiles) < 1:
            log.warning('No tiles passed filtering, nothing to add to the database')
            return

        # Get the database User, or make it if it doesn't exist
        try:
            db_user = db.get_user(session, username='sentinel')
        except ValueError:
            db_user = db.User('sentinel', '', 'Sentinel Alert Listener')

        # Create Targets for each tile
        db_targets = []
        for tile_name, _, _, tile_weight in selected_tiles:
            # Find the matching GridTile
            query = session.query(db.GridTile)
            query = query.filter(db.GridTile.grid == db_grid)
            query = query.filter(db.GridTile.name == str(tile_name))
            db_grid_tile = query.one_or_none()

            # Create ExposureSets
            db_exposure_sets = []
            for exposure_sets_dict in event.strategy_dict['exposure_sets_dict']:
                db_exposure_sets.append(
                    db.ExposureSet(
                        num_exp=exposure_sets_dict['num_exp'],
                        exptime=exposure_sets_dict['exptime'],
                        filt=exposure_sets_dict['filt'],
                    )
                )

            # Create Strategies
            constraints_dict = event.strategy_dict['constraints_dict']
            if isinstance(event.strategy_dict['cadence_dict'], dict):
                cadence_dicts = [event.strategy_dict['cadence_dict']]
            else:
                cadence_dicts = event.strategy_dict['cadence_dict']
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
                    rank=event.strategy_dict['rank'],
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

    return grid


def handle_event(event, send_messages=False, ignore_test=True, log=None, time=None):
    """Handle a new Event.

    Parameters
    ----------
    event : `gototile.events.Event`
        The Event to handle

    send_messages : bool, optional
        If True, send Slack messages.
        Default is False.
    ignore_test : bool, optional
        If True, ignore events with the 'test' role.
        Default is True.
    log : logging.Logger, optional
        If given, direct log messages to this logger.
        If None, a new logger is created.
    time : astropy.time.Time, optional
        If given, insert entries at the given time (useful for testing).
        If None, use the current time.

    Returns
    -------
    processed : bool
        Will return True if the Event was processed.

    """
    if log is None:
        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('event_handler')
        log.setLevel(level=logging.DEBUG)
    if time is None:
        time = Time.now()
    ignore_roles = ['utility']  # We never want to process utility events
    if ignore_test:
        ignore_roles.append('test')

    log.info('Handling Event {}'.format(event.ivorn))

    # 1) Check if it's an event we want to process, otherwise return here
    if event.type == 'unknown' or event.role in ignore_roles:
        log.warning(f'Ignoring {event.type} {event.role} event')
        return False
    log.info('Processing {} Event {}'.format(event.type, event.name))

    # 2) Send the initial Slack message
    #    We send this first so if downloading the skymap fails we already have some sort of record
    if send_messages:
        log.debug('Sending initial Slack report')
        if event.role == 'observation':
            msg = f'*Processing new {event.source} {event.type} event: {event.id}*'
        else:
            msg = f'*Processing new {event.source} {event.type} {event.role} event: {event.id}*'
        send_slack_msg(msg)
        log.debug('Slack report sent')

    # 3) Fetch the event skymap
    #    We do this here so that we don't bother downloading for events we rejected already
    log.info('Fetching event skymap')
    event.get_skymap()
    log.debug('Skymap created')

    # 4) Add to the alert database
    #    We have to do this after we've downloaded the skymap
    #    TODO: What about the survey ID?
    log.info('Adding event to the alert database')
    try:
        event.add_to_database()
        log.debug('Event added to the database')
    except Exception as err:
        log.error('Error adding event to the database')
        log.debug(err.__class__.__name__, exc_info=True)

    # 5) Send the event & strategy reports to Slack
    #    Retractions have no strategy, so we only send one report for them
    if send_messages:
        log.debug('Sending Slack event report')
        try:
            send_event_report(event)
            if event.strategy is not None:
                send_strategy_report(event)
            log.debug('Slack reports sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # 6) Create targets and add them into the observation database
    #    (after removing previous targets for the same event (which is all we do for retractions))
    #    TODO: Do the tiling in a separate step?
    log.info('Adding targets to the observation database')
    if event.strategy is not None:  # Retractions have no strategy
        log.info('Using strategy {}'.format(event.strategy))
    try:
        # Need to regrade if the skymap is too big
        # (note we do this after adding the origional to the alert database)
        # TODO: This could be in the tiling step
        if (event.skymap is not None and event.skymap.is_moc is False and
                (event.skymap.nside > 128 or event.skymap.order == 'RING')):
            event.skymap.regrade(nside=128, order='NESTED')
        grid = add_event_to_obsdb(event, time, log)
        log.info('Targets added to the database')
    except Exception as err:
        log.error('Unable to insert event into database')
        log.debug(err.__class__.__name__, exc_info=True)
        if send_messages:
            log.debug('Sending Slack error report')
            msg = '*ERROR*: Failed to insert event {} into database'.format(event.name)
            send_slack_msg(msg)
            log.debug('Slack report sent')

    # 7) Send the database report to Slack
    if send_messages:
        log.debug('Sending Slack database report')
        try:
            # TODO: If adding the event fails then the grid won't be defined
            #       (see splitting off the tiling step above)
            send_database_report(event, grid, time=time)
            log.debug('Slack report sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # Done
    log.info('Event {} processed'.format(event.name))
    return True
