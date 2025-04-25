"""Functions for handling notices."""

import logging

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as obs_db

from . import database as alert_db
from .coincidence import find_coincident_events
from .slack import send_notice_report, send_observing_report, send_slack_msg


def already_in_database(notice):
    """Check if the given notice already exists in the alert database."""
    with alert_db.session_manager() as session:
        query = session.query(alert_db.Notice)
        query = query.filter(alert_db.Notice.ivorn == notice.ivorn)
        db_notice = query.one_or_none()
        if db_notice is not None:
            return True
    return False


def add_notice_to_database(notice, log=None):
    """Add an entry for the given Notice to the alert database.

    If a matching Event already exists, the Notice will be linked to it.
    If not, a new Event will be created.
    """
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    with alert_db.session_manager() as session:
        # Get any matching Event from the database, or make one if it's new
        query = session.query(alert_db.Event)
        query = query.filter(alert_db.Event.name == notice.event_name)
        db_event = query.one_or_none()
        if db_event is None:
            db_event = alert_db.Event(
                name=notice.event_name,
                type=notice.event_type,
                origin=notice.source,
                time=notice.event_time,
            )
            log.debug(f'Adding event {db_event.name} to database')
        else:
            log.debug(f'Matching to existing event {db_event.name}')

        # Make sure we have the notice skymap
        if notice.skymap is None:
            notice.get_skymap()  # May still be None if it's a retraction

        # Now add the Notice (no Survey ID for now, we'll update that later if we add one)
        # TODO: We should store the Skymap in its own table
        # TODO: Also we should store other supplementary data like the GWSkyNet scores somewhere
        #       Could its own table, or as an extra field in the Notice table
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

        return db_notice.db_id


def delete_survey_targets(db_survey, time=None, log=None):
    """Delete all Targets for the given Survey."""
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    with alert_db.session_manager() as session:
        num_deleted = 0
        for db_target in db_survey.targets:
            statuses = ['deleted', 'expired', 'completed']
            if db_target.status_at_time(time) not in statuses:
                db_target.mark_deleted(time=time)
                num_deleted += 1
        if num_deleted > 0:
            log.debug(f'Deleted {num_deleted} Targets for Survey {db_survey.name}')
        session.commit()


def add_survey_to_database(notice, time=None, log=None):
    """Add a Survey for the given Notice to the observation database.

    This assumes we've already added the Notice to the alert database.

    If the Event for this notice has any previous surveys, then
    we'll check if the skymap or strategy has changed.
    If so, we'll create a new Survey and delete the Targets for all previous ones.

    If the notice has the IGNORE or RETRACTION strategy then no new Survey is created,
    but we'll still delete any incomplete Targets from previous Surveys.
    """
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    with alert_db.session_manager() as session:
        # First get the Event from the database
        db_notice = session.query(alert_db.Notice).filter_by(ivorn=notice.ivorn).one()

        # Now find how many previous Surveys have been defined for this Event (if any)
        db_event = db_notice.event
        log.debug(f'Found {len(db_event.surveys)} previous surveys for this event')

        requires_update = False
        if len(db_event.surveys) == 0:
            # There are no previous Surveys, so we'll want to create one.
            requires_update = True
        else:
            # There are existing Surveys for this Event.
            # We want to see if the skymap or strategy has changed from the previous notice.
            # If it has, we'll want to create a new survey and targets and remove the old one.
            last_db_notice = db_event.notices[-2]  # (-1 should be this one)
            last_notice = last_db_notice.gcn
            log.debug(f'Previous notice {last_notice.ivorn} was received at {last_notice.time}')

            # Check if the skymap has changed
            if last_notice.skymap != notice.skymap:
                log.info('Event skymap has been updated')
                requires_update = True
            else:
                log.info('Event skymap has not changed')

            # Check if the strategy has changed
            if last_notice.strategy != notice.strategy:
                msg = 'Event strategy has changed'
                msg += f' from {last_notice.strategy} to {notice.strategy}'
                log.info(msg)
                requires_update = True
            else:
                log.info(f'Event strategy remains as {notice.strategy}')

        if requires_update:
            # Go through any previous Surveys for this Event and "delete" any incomplete Targets.
            # Using target.mark_deleted() will also delete any pending Pointings,
            # but won't interrupt one if it's currently running.
            # If there are multiple previous Surveys then all but the latest should have already
            # been deleted, but we might as well go through and check to be sure.
            for db_survey in db_event.surveys:
                delete_survey_targets(db_survey, time, log)

        if notice.strategy in ['IGNORE', ' RETRACTION'] or notice.strategy_dict is None:
            # We've added the Notice to the database, but we don't need to add a new Survey.
            # After deleting any old Targets above there's nothing else to do here.
            return None

        if not requires_update:
            # Nothing new to add, we just need to link this Notice to the latest Survey.
            db_notice.survey_id = db_event.surveys[-1].db_id
            return None

        # Otherwise we know this notice has a new skymap (or strategy),
        # so we want to create a new Survey.
        db_survey = obs_db.Survey(name=f'{notice.event_name}_{len(db_event.surveys) + 1}')
        log.debug(f'Adding survey {db_survey.name} to database')
        session.add(db_survey)
        session.commit()
        survey_id = db_survey.db_id

        # Finally link the Notice to the new Survey.
        db_notice.survey_id = survey_id

    return survey_id


def check_coincident_events(notice, time_window=10, skymap_contour=0.95, time=None, log=None):
    """Check the AlertDB for any events with matching times and tile sets.

    Returns True if a better event was found, False if not.

    If any overlapping events are found we check which notice covers the fewest tiles.
    If the new notice covers fewer tiles: delete the older notice and its targets,
    and return False.
    If the new notice covers the same or more tiles: keep the older targets and return True
    (to signify no new Targets need to be added).

    If no overlapping events are found then also return False.

    """
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    # Find any overlapping events
    coincident_events = find_coincident_events(
        notice, time_window=time_window, skymap_contour=skymap_contour, return_parameters=True
    )
    if len(coincident_events) == 0:
        # No matching events found, so nothing to do
        log.debug('No coincident events found')
        return False
    log.info(f'Found {len(coincident_events)} coincident events')

    # There are matching events, great!
    # Now we want to go through each of them and log the coincidence, then check if any of
    # their notices are are "better" than this one.
    with alert_db.session_manager() as session:
        # Get the event for the current notice
        db_notice = session.query(alert_db.Notice).filter_by(ivorn=notice.ivorn).one()
        db_event = db_notice.event

        found_better = False
        for matched_event_id, time_diff, skymap_overlap, tile_overlap in coincident_events:
            # Get the event and latest notice from the database
            matched_event = session.query(alert_db.Event).filter_by(db_id=matched_event_id).one()
            log.info(f'Coincident event: {matched_event.name}')

            # Create a Coincidence group for this Event, or add it to an existing one
            if matched_event.coincidence is None and db_event.coincidence is None:
                # Create a new Coincidence group
                db_coincidence = alert_db.Coincidence()
                session.add(db_coincidence)
                session.commit()
                # Link this old event and the new one to the new Coincidence group
                matched_event.coincidence = db_coincidence
                db_event.coincidence = db_coincidence
                log.debug(f'Creating new Coincidence group {db_coincidence.db_id}')
            elif db_event.coincidence is None:
                # The matched Event is already part of a Coincidence group,
                # so we just need to add our new event to it
                db_event.coincidence = matched_event.coincidence
                log.debug(f'Extending Coincidence group {matched_event.coincidence.db_id}')
            else:
                # Somehow our new Event is already in a Coincidence group.
                # This could happen if we're looping through matched events, and we've just
                # added or created a new group.
                # Or this is an update notice for an already-existing event.
                if matched_event.coincidence == db_event.coincidence:
                    # That's fine, they're already in the same group.
                    log.debug(f'Already in Coincidence group {db_event.coincidence.db_id}')
                elif matched_event.coincidence is None:
                    # The matched Event is not in a Coincidence group, so we can add it to ours.
                    # This could happen if Event A and Event B weren't matched,
                    # now Event C comes along and matches both. So we created a new group with
                    # Event A, and now we backfill it onto Event B.
                    matched_event.coincidence = db_event.coincidence
                    log.debug(f'Backfilling Coincidence group {db_event.coincidence.db_id}')
                else:
                    # Here we have a problem, our new Event is in a different group
                    # than this one that was matched to it.
                    # This should be very uncommon, but it could happen e.g.
                    # - Event A and B have identical timestamps and are grouped together,
                    # - Event C and D have a different timestamp 11 seconds away from the first,
                    #   so they're grouped together into a new group,
                    # - Event E has appears in between the two times, so is matched to all 4.
                    # (this is assuming all their skymaps all overlap).
                    # The matched events should be sorted by time, so if we've already matched
                    # this new event to a group then we should keep it and move this matched event
                    # into it.
                    log.warning('Multiple Coincidence groups detected!')
                    msg = f'Merging {matched_event.coincidence.db_id}'
                    msg += ' into {db_event.coincidence.db_id}'
                    log.warning(msg)
                    # Now we have a problem because there might be other events in the second
                    # group which weren't matched to this new notice!
                    # So we need to query for them and change their group too.
                    query = session.query(alert_db.Event)
                    query = query.filter(alert_db.Event.coincidence==matched_event.coincidence)
                    query = query.filter(alert_db.Event!=matched_event)
                    grouped_events = query.all()
                    # Now replace the groups for these events
                    log.debug(f'Backfilling Coincidence group {db_event.coincidence.db_id}')
                    matched_event.coincidence = db_event.coincidence
                    for grouped_event in grouped_events:
                        log.debug(f'Backfilling secondary Event {db_event.name}')
                        grouped_event.coincidence = db_event.coincidence
                    # This will leave an empty Coincidence row, which is a bit awkward,
                    # but without some many-to-many option of Events being part of multiple groups
                    # there's no easier option.

            # Check if the notice has any targets
            if len(matched_event.notices[-1].targets) == 0:
                # No targets for this notice (might have been below min_prob), so we can ignore it
                log.info('Notice has no targets defined')
                continue

            # Now we can actually look at the matched notice
            # TODO: ADD SLACK MESSAGE
            matched_notice = matched_event.notices[-1].gcn
            log.info(f'Latest notice: {matched_notice.ivorn}')
            log.debug(f'Time difference: {time_diff:.3f}s')
            log.debug(f'Skymap overlap: {skymap_overlap:.0%}')
            log.debug(f'Tile overlap: {tile_overlap:.0%}')

            # Now we want to decide which notice is best to observe.
            # It's not actually that obvious. We could compare the size of the skymaps or
            # the number of tiles, but thanks to different selection limits you could have a
            # case where a smaller skymap had more tiles added to the ObsDB.
            # Easiest is just to go with the skymap area for which event better localised and
            # therefore closer to the "real" position.
            old_area = matched_notice.skymap.get_contour_area(skymap_contour)
            new_area = notice.skymap.get_contour_area(skymap_contour)
            log.debug(f'Old skymap area: {old_area:.2f} deg2')
            log.debug(f'New skymap area: {new_area:.2f} deg2')
            if new_area < old_area:
                # This new notice is smaller, so we want to keep it and delete all the old targets
                # for the previous event.
                # If there are multiple previous Surveys then all but the latest should have already
                # been deleted, but we might as well go through and check to be sure.
                log.info('New notice skymap is smaller, deleting existing targets')
                for db_survey in db_event.surveys:
                    delete_survey_targets(db_survey, time, log)
            else:
                # The new notice is worse, so we want to ignore it and keep the existing targets.
                log.info('New notice skymap is larger, leaving existing targets')
                found_better = True

        return found_better

def add_targets_to_database(notice, time=None, log=None):
    """Add Targets for the given notice to the observation database.

    This assumes we've already added the Notice to the alert database,
    and created a new Survey for it.

    """
    if time is None:
        time = Time.now()
    if log is None:
        log = logging.getLogger('database')
        log.setLevel(level=logging.DEBUG)

    # We should have already selected the tiles for this notice
    selected_tiles = notice.select_tiles()

    # It's possible no tiles passed the selection criteria,
    # if so then there's nothing else to do.
    if len(selected_tiles) < 1:
        log.warning('Nothing to add to the database')
        return []

    # Create and add new Targets (and related entries) into the observation database
    with obs_db.session_manager() as session:
        # Get the database User (make it if it doesn't exist) and the current Grid,
        # so we can link them to the new Targets
        try:
            db_user = obs_db.get_user(session, username='sentinel')
        except ValueError:
            db_user = obs_db.User('sentinel', '', 'Sentinel alert Listener')
        db_grid = obs_db.get_current_grid(session)

        # Get the survey ID from the notice
        db_notice = session.query(alert_db.Notice).filter_by(ivorn=notice.ivorn).one()
        survey_id = db_notice.survey_id

        # Create entries for each tile
        db_targets = []
        for tile_name, tile_weight in selected_tiles[('tilename', 'prob')]:
            # Find the matching GridTile
            query = session.query(obs_db.GridTile)
            query = query.filter(obs_db.GridTile.grid == db_grid)
            query = query.filter(obs_db.GridTile.name == str(tile_name))
            db_grid_tile = query.one_or_none()

            # Create ExposureSets
            db_exposure_sets = []
            for exposure_set in notice.strategy_dict['exposure_sets']:
                db_exposure_sets.append(
                    obs_db.ExposureSet(
                        num_exp=exposure_set['num_exp'],
                        exptime=exposure_set['exptime'],
                        filt=exposure_set['filt'],
                    )
                )

            # Create Strategies
            constraints = notice.strategy_dict['constraints']
            if isinstance(notice.strategy_dict['cadence'], dict):
                cadences = [notice.strategy_dict['cadence']]
            else:
                cadences = notice.strategy_dict['cadence']
            db_strategies = []
            for cadence in cadences:
                db_strategies.append(
                    obs_db.Strategy(
                        num_todo=cadence['num_todo'],
                        stop_time=cadence['stop_time'],
                        wait_time=cadence['wait_hours'] * u.hour,
                        valid_time=None,  # Pointings are valid up until the stop_time
                        rank_change=cadence['rank_change'],
                        min_time=None,
                        too=True,
                        min_alt=constraints['min_alt'],
                        max_sunalt=constraints['max_sunalt'],
                        max_moon=constraints['max_moon'],
                        min_moonsep=constraints['min_moonsep'],
                        # TODO: tel_mask?
                    )
                )

            # Create Targets (this will automatically create Pointings)
            # NB we take the earliest start time and latest stop time from all cadences,
            # in case there's more than one.
            db_targets.append(
                obs_db.Target(
                    name=f'{notice.event_name}_{tile_name}',
                    ra=None,  # RA/Dec are inherited from the grid tile
                    dec=None,
                    rank=notice.strategy_dict['rank'],
                    weight=float(tile_weight),
                    start_time=min(c['start_time'] for c in cadences),
                    stop_time=max(c['stop_time'] for c in cadences),
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
        log.info(f'Adding {len(db_targets)} targets to the database')
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise

        return [target.db_id for target in db_targets]


def handle_notice(notice, send_messages=False, log=None, time=None):
    """Handle a new transient notice.

    Parameters
    ----------
    notice : `gtecs.alert.notices.Notice` or subclass
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

    log.info(f'Handling notice {notice.ivorn}')

    # Check if the notice is already in the database.
    # Do this before fetching the skymap to save time.
    if already_in_database(notice):
        log.info('Notice already in the alert database')
        return

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
    # First, add the Notice to the alert database (and create a new Event if needed)
    add_notice_to_database(notice, log=log)

    # Then create a new Survey, deleting any previous targets if needed
    survey_id = add_survey_to_database(notice, time=time, log=log)
    if survey_id is None:
        # We didn't add a new Survey, either it's a retraction or there have been no changes
        if notice.strategy in ['IGNORE', ' RETRACTION'] or notice.strategy_dict is None:
            log.info(f'{notice.strategy} notice processed')
            return
        else:
            log.info('No changes to the skymap or strategy, so no update to the database required')
            return

    # Select the grid tiles for this notice
    log.debug('Selecting grid tiles')
    selected_tiles = notice.select_tiles()
    log.debug(f'Selected {len(selected_tiles)} tiles')

    # Now check for overlaps
    found_better_event = check_coincident_events(notice, time=time, log=log)
    if found_better_event:
        # There was a matching event in the database that covers fewer tiles,
        # so we don't want to add this one.
        log.info('Ignoring notice in favor of previous event')
    else:
        # Add new Targets for the survey
        add_targets_to_database(notice, time=time, log=log)

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

    log.info(f'Notice {notice.ivorn} successfully processed')
    return
