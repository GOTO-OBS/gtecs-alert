"""Functions for handling notices."""

import logging

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as obs_db

from . import database as alert_db
from .slack import send_database_report, send_event_report, send_strategy_report


def add_to_database(event, time=None, log=None):
    """Add entries for this event into the database(s)."""
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    # First make sure we have the event skymap
    if event.skymap is None:
        event.get_skymap()  # May still be None if it's a retraction

    # Add to the alert database
    with alert_db.open_session() as session:
        # Get any matching Event from the database, or make one if it's new
        query = session.query(alert_db.Event)
        query = query.filter(alert_db.Event.name == event.name)
        db_event = query.one_or_none()
        if db_event is None:
            db_event = alert_db.Event(
                name=event.name,
                type=event.type,
                origin=event.source,
                time=event.time,
            )

        # Now add the Notice (we'll update the survey ID later)
        db_notice = alert_db.Notice.from_gcn(event)
        db_notice.event = db_event
        session.add(db_notice)

        # Find how many previous surveys there have been for this event
        event_surveys = [survey.db_id for survey in db_event.surveys]
        log.debug(f'Found {len(event_surveys)} previous surveys for this event')

        if len(event_surveys) > 0:
            # We want to see if the skymap or strategy has changed from the previous notice.
            # If it has, we'll want to create a new survey.
            # If there are previous surveys for this event, there should be previous notices.
            last_notice = db_event.notices[-2]  # (-1 would be the one we just created)
            log.debug(f'Previous notice {last_notice.ivorn} was received at {last_notice.received}')
            last_event = last_notice.gcn_event
            requires_update = False
            if last_event.skymap != event.skymap:
                log.info('Event skymap has been updated')
                requires_update = True
            else:
                log.info('Event skymap has not changed')
            if last_event.strategy != event.strategy:
                msg = f'Event strategy has changed from {last_event.strategy} to {event.strategy}'
                log.info(msg)
                requires_update = True
            else:
                log.info(f'Event strategy remains as {event.strategy}')

            if requires_update:
                # Go through previous Surveys for this Event and "delete" any incomplete Targets.
                # Using target.mark_deleted() will also delete any pending Pointings,
                # but won't interrupt one if it's currently running.
                # If there are multiple previous Surveys then all but the latest should have already
                # been deleted, but we might as well go through and check to be sure.
                for db_survey in db_event.surveys:
                    num_deleted = 0
                    for db_target in db_survey.targets:
                        statuses = ['deleted', 'expired', 'completed']
                        if db_target.status_at_time(time) not in statuses:
                            db_target.mark_deleted(time=time)
                            num_deleted += 1
                    if num_deleted > 0:
                        log.debug(f'Deleted {num_deleted} Targets for Survey {db_survey.name}')
                    session.commit()
        else:
            # If there are no previous surveys, we'll want to create one.
            requires_update = True

    if requires_update is False:
        log.info('No changes to the event, so no update to the database required')
        return
    if event.strategy is None:
        # It's a retraction event
        log.info('Retraction event processed')
        return
    elif event.skymap is None:
        # We have a strategy but no skymap, so we can't do anything?
        raise ValueError('No skymap for event {}'.format(event.name))

    # We know this notice has a new skymap (or strategy) so we want to create a new Survey.
    with obs_db.open_session() as session:
        db_survey = obs_db.Survey(
            name=f'{event.name}_{len(event_surveys) + 1}',
        )
        log.debug('Adding Survey {} to database'.format(db_survey.name))
        session.add(db_survey)
        session.commit()
        survey_id = db_survey.db_id

    # Update the Survey ID in the alert database, so we can map between the objects
    with alert_db.open_session() as session:
        db_notice = session.query(alert_db.Notice).filter_by(ivorn=event.ivorn).one()
        db_notice.survey_id = survey_id

    # Now select the grid tiles covering the skymap
    log.debug('Selecting grid tiles')
    with obs_db.open_session() as session:
        db_grid = obs_db.get_current_grid(session)
        grid = db_grid.skygrid
    # If the skymap is too big we regrade before applying it to the grid
    # (note that we do only this after adding the original skymap to the alert database)
    if (event.skymap is not None and event.skymap.is_moc is False and
            (event.skymap.nside > 128 or event.skymap.order == 'RING')):
        event.skymap.regrade(nside=128, order='NESTED')
    # Apply the skymap to the grid
    grid.apply_skymap(event.skymap)
    # Get the grid tiles covering the skymap for a given contour level
    # TODO: The selection contour is currently fixed, but it should be based on simulations
    #       and could change based on the type of event (part of strategy?)
    contour_level = 0.95
    selected_tiles = grid.select_tiles(
        contour=contour_level,
        max_tiles=event.strategy_dict['tile_limit'],
        min_tile_prob=event.strategy_dict['prob_limit'],
    )
    selected_tiles.sort('prob')
    selected_tiles.reverse()
    log.debug('Selected {}/{} tiles'.format(len(selected_tiles), grid.ntiles))
    # It's possible no tiles passed the selection criteria,
    # if so then there's nothing else to do (but we still add the "empty" survey above)
    if len(selected_tiles) < 1:
        log.warning('Nothing to add to the database')
        return

    # Create and add new Targets (and related entries) into the observation database
    with obs_db.open_session() as session:
        # Get the database User (make it if it doesn't exist) and the current Grid,
        # so we can link them to the new Targets
        try:
            db_user = obs_db.get_user(session, username='sentinel')
        except ValueError:
            db_user = obs_db.User('sentinel', '', 'Sentinel alert Listener')
        db_grid = obs_db.get_current_grid(session)

        # Create Targets for each tile
        db_targets = []
        for tile_name, _, _, tile_weight in selected_tiles:
            # Find the matching GridTile
            query = session.query(obs_db.GridTile)
            query = query.filter(obs_db.GridTile.grid == db_grid)
            query = query.filter(obs_db.GridTile.name == str(tile_name))
            db_grid_tile = query.one_or_none()

            # Create ExposureSets
            db_exposure_sets = []
            for exposure_sets_dict in event.strategy_dict['exposure_sets_dict']:
                db_exposure_sets.append(
                    obs_db.ExposureSet(
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
                    obs_db.Strategy(
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

            # Create Targets (this will automatically create Pointings)
            # NB we take the earliest start time and latest stop time from all cadences
            db_targets.append(
                obs_db.Target(
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
                    survey_id=survey_id,
                )
            )

        # Add everything to the database
        log.debug('Adding {} Targets to the database'.format(len(db_targets)))
        obs_db.insert_items(session, db_targets)

        # Commit changes
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise

    # Return the grid that has had the skymap applied for the Slack message
    return grid


def handle_event(event, send_messages=False, ignore_test=True, log=None, time=None):
    """Handle a new Event.

    Parameters
    ----------
    event : `gototile.gcn.Event`
        The Event to handle

    send_messages : bool, optional
        If True, send Slack messages.
        Default is False.
    ignore_test : bool, optional
        If True, ignore notices with the 'test' role.
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
    ignore_roles = ['utility']  # We never want to process utility notices
    if ignore_test:
        ignore_roles.append('test')

    log.info('Handling Event {}'.format(event.ivorn))

    # 0) Check if it's an event we want to process, otherwise return here
    #    TODO: should this be within the sentinel?
    if event.type == 'unknown' or event.role in ignore_roles:
        log.warning(f'Ignoring {event.type} {event.role} event')
        return False
    log.info('Processing {} Event {}'.format(event.type, event.name))

    # 1) Fetch the event skymap
    #    We do this here so that we don't bother downloading for notices we have already rejected
    log.info('Fetching event skymap')
    event.get_skymap()
    log.debug('Skymap created')

    # 2) Send the event & strategy reports to Slack
    #    Retractions have no strategy, so we only send one report for them
    if send_messages:
        log.debug('Sending Slack event report')
        try:
            send_event_report(event)
            if event.strategy is not None:
                log.info('Using strategy {}'.format(event.strategy))
                send_strategy_report(event)
            log.debug('Slack reports sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # 3) Add the entries to the database
    log.info('Adding notice to the alert database')
    grid = add_to_database(event, time=time)

    # 4) Send the database report to Slack
    # TODO: Should we have some checks that the database was updated correctly here,
    #       instead of just leaving it to the Slack report?
    if send_messages:
        log.debug('Sending Slack database report')
        try:
            send_database_report(event, grid, time=time)
            log.debug('Slack report sent')
        except Exception as err:
            log.error('Error sending Slack report')
            log.debug(err.__class__.__name__, exc_info=True)

    # Done
    log.info('Event {} processed'.format(event.name))
    return True
