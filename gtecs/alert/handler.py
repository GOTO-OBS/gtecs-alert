"""Functions for handling notices."""

import logging

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as obs_db

from . import database as alert_db
from .slack import send_notice_report, send_observing_report, send_slack_msg


def add_to_database(notice, time=None, log=None):
    """Add entries for this notice into the database(s)."""
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    # First make sure we have the skymap
    if notice.skymap is None:
        notice.get_skymap()  # May still be None if it's a retraction

    # Add to the alert database
    with alert_db.session_manager() as session:
        # Get any matching Event from the database, or make one if it's new
        query = session.query(alert_db.Event)
        query = query.filter(alert_db.Event.name == notice.event_name)
        db_event = query.one_or_none()
        if db_event is None:
            db_event = alert_db.Event(
                name=notice.event_name,
                type=notice.event_type,
                origin=notice.event_source,
                time=notice.event_time,
            )

        # Now add the Notice (we'll update the survey ID later)
        db_notice = alert_db.Notice.from_gcn(notice)
        db_notice.event = db_event
        try:
            session.add(db_notice)
            session.commit()
        except Exception as err:
            if 'duplicate key value violates unique constraint "notices_ivorn_key"' in str(err):
                raise ValueError('Notice already exists in alert database') from err
            else:
                raise

        # Find how many previous surveys there have been for this event
        event_surveys = [survey.db_id for survey in db_event.surveys]
        log.debug(f'Found {len(event_surveys)} previous surveys for this event')

        if len(event_surveys) > 0:
            # We want to see if the skymap or strategy has changed from the previous notice.
            # If it has, we'll want to create a new survey.
            # If there are previous surveys for this event, there should be previous notices.
            last_dbnotice = db_event.notices[-2]  # (-1 would be the one we just created)
            last_notice = last_dbnotice.gcn
            log.debug(f'Previous notice {last_notice.ivorn} was received at {last_notice.time}')
            requires_update = False
            if last_notice.skymap != notice.skymap:
                log.info('Event skymap has been updated')
                requires_update = True
            else:
                log.info('Event skymap has not changed')
            if last_notice.strategy != notice.strategy:
                msg = f'Event strategy has changed from {last_notice.strategy} to {notice.strategy}'
                log.info(msg)
                requires_update = True
            else:
                log.info(f'Event strategy remains as {notice.strategy}')

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

    if notice.strategy in ['IGNORE', ' RETRACTION'] or notice.strategy_dict is None:
        # Either it's an event we don't care about, or it's an explicit retraction notice.
        # We've added it to the AlertDB and deleted the previous Targets, nothing else to do.
        log.info(f'{notice.strategy} notice processed')
        return
    elif notice.skymap is None:
        # We have a strategy but no skymap, so we can't do anything?
        raise ValueError('Notice has a strategy but no skymap')

    if requires_update is True:
        # We know this notice has a new skymap (or strategy) so we want to create a new Survey.
        with obs_db.session_manager() as session:
            db_survey = obs_db.Survey(
                name=f'{notice.event_name}_{len(event_surveys) + 1}',
            )
            log.debug('Adding Survey {} to database'.format(db_survey.name))
            session.add(db_survey)
            session.commit()
            survey_id = db_survey.db_id
    else:
        # The existing Survey is fine, just get the ID.
        with obs_db.session_manager() as session:
            query = session.query(obs_db.Survey)
            query = query.filter_by(name=f'{notice.event_name}_{len(event_surveys)}')
            db_survey = query.one()
            survey_id = db_survey.db_id

    # Update the Survey ID in the alert database, so we can map between the objects
    with alert_db.session_manager() as session:
        db_notice = session.query(alert_db.Notice).filter_by(ivorn=notice.ivorn).one()
        db_notice.survey_id = survey_id

    if requires_update is False:
        log.info('No changes to the skymap or strategy, so no update to the database required')
        return

    # Now select the grid tiles covering the skymap
    log.debug('Selecting grid tiles')
    with obs_db.session_manager() as session:
        db_grid = obs_db.get_current_grid(session)
        grid = db_grid.skygrid
    # If the skymap is too big we regrade before applying it to the grid
    # (note that we do only this after adding the original skymap to the alert database)
    if (notice.skymap is not None and notice.skymap.is_moc is False and
            (notice.skymap.nside > 128 or notice.skymap.order == 'RING')):
        notice.skymap.regrade(nside=128, order='NESTED')
    # Apply the skymap to the grid
    grid.apply_skymap(notice.skymap)
    # Get the grid tiles covering the skymap for a given contour level
    selected_tiles = grid.select_tiles(
        contour=notice.strategy_dict['skymap_contour'],
        max_tiles=notice.strategy_dict['tile_limit'],
        min_tile_prob=notice.strategy_dict['prob_limit'],
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
    with obs_db.session_manager() as session:
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
            for exposure_sets_dict in notice.strategy_dict['exposure_sets_dict']:
                db_exposure_sets.append(
                    obs_db.ExposureSet(
                        num_exp=exposure_sets_dict['num_exp'],
                        exptime=exposure_sets_dict['exptime'],
                        filt=exposure_sets_dict['filt'],
                    )
                )

            # Create Strategies
            constraints_dict = notice.strategy_dict['constraints_dict']
            if isinstance(notice.strategy_dict['cadence_dict'], dict):
                cadence_dicts = [notice.strategy_dict['cadence_dict']]
            else:
                cadence_dicts = notice.strategy_dict['cadence_dict']
            db_strategies = []
            for cadence_dict in cadence_dicts:
                db_strategies.append(
                    obs_db.Strategy(
                        num_todo=cadence_dict['num_todo'],
                        stop_time=cadence_dict['stop_time'],
                        wait_time=cadence_dict['wait_hours'] * u.hour,
                        valid_time=None,  # Pointings are valid up until the stop_time
                        rank_change=cadence_dict['rank_change'],
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
                    name=f'{notice.event_name}_{tile_name}',
                    ra=None,  # RA/Dec are inherited from the grid tile
                    dec=None,
                    rank=notice.strategy_dict['rank'],
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

            # Add the target to the database (and all related entries)
            session.add(db_targets[-1])

        # Commit changes
        log.debug('Adding {} Targets to the database'.format(len(db_targets)))
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise


def handle_notice(notice, send_messages=False, log=None, time=None):
    """Handle a new GCN notice.

    Parameters
    ----------
    notice : `gototile.gcn.GCNNotice` or subclass
        The notice to handle

    send_messages : bool, optional
        If True, send Slack messages.
        Default is False.
    log : logging.Logger, optional
        If given, direct log messages to this logger.
        If None, a new logger is created.
    time : astropy.time.Time, optional
        If given, insert entries at the given time (useful for testing).
        If None, use the current time.

    """
    if log is None:
        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('handler')
        log.setLevel(level=logging.DEBUG)
    if time is None:
        time = Time.now()

    log.info('Handling notice {}'.format(notice.ivorn))

    log.info('Fetching skymap')
    notice.get_skymap()
    if send_messages:
        log.debug('Sending Slack notice report')
        try:
            send_notice_report(notice, time=time)
        except Exception as err:
            log.exception('Error sending notice report')
            try:
                msg = f'Error sending notice report ("{err.__class__.__name__}: {err}")'
                send_slack_msg(msg)
            except Exception:
                log.exception('Error sending error report')

    log.info('Adding notice to the alert database')
    add_to_database(notice, time=time, log=log)

    if send_messages:
        log.debug('Sending Slack observing report')
        try:
            send_observing_report(notice, time=time)
        except Exception as err:
            log.exception('Error sending observing report')
            try:
                msg = f'Error sending observing report ("{err.__class__.__name__}: {err}")'
                send_slack_msg(msg)
            except Exception:
                log.exception('Error sending error report')

    log.info('Notice {} successfully processed'.format(notice.ivorn))
    return
