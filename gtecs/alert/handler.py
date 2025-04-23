"""Functions for handling notices."""

import logging

from astropy import units as u
from astropy.time import Time

from gtecs.obs import database as obs_db

import numpy as np

from . import database as alert_db
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


def get_skymap_overlap(skymap1, skymap2, contour=0.95, regrade_nside=128):
    """Get the overlap fraction between two skymaps at a given contour level.

    TODO: This could be a method on the gototile.SkyMap class
    """
    # Regrade each skymap to the same nside
    # TODO: It would be nice if it returned a new skymap (with inplace=False)
    #       instead of modifying the original one.
    if skymap1.nside != regrade_nside:
        skymap1 = skymap1.copy()
        skymap1.regrade(nside=regrade_nside)
    if skymap2.nside != regrade_nside:
        skymap2 = skymap2.copy()
        skymap2.regrade(nside=regrade_nside)

    # Get the pixel indices within the contour level
    contour_ipix1 = set(np.where(skymap1.contours<contour)[0])
    contour_ipix2 = set(np.where(skymap2.contours<contour)[0])
    intersection = contour_ipix1.intersection(contour_ipix2)
    if len(intersection) == 0:
        return 0.0

    # We want to return the biggest overlap fraction
    # (i.e. the fraction of the smaller skymap that overlaps with the larger one)
    return len(intersection) / min(len(contour_ipix1), len(contour_ipix2))


def get_tile_overlap(notice1, notice2):
    """Get the overlap fraction between two notice tilesets.

    Returns the fraction of the smaller set that overlaps with the larger one.
    """
    selected1 = notice1.select_tiles()
    selected2 = notice2.select_tiles()
    tile_set1 = set(selected1['tilename'])
    tile_set2 = set(selected2['tilename'])
    intersection = tile_set1.intersection(tile_set2)
    if len(intersection) == 0:
        return 0.0

    # We want to return the biggest overlap fraction
    # (i.e. the fraction of the smaller set that overlaps with the larger one)
    return len(intersection) / min(len(tile_set1), len(tile_set2))


def find_coincident_events(notice, time_window=10, skymap_contour=0.95, type_limit=True):
    """Check the AlertDB for any other events with matching times and positions.

    Parameters
    ----------
    notice : `gtecs.alert.notices.Notice`
        The notice to check for coincidences with.

    time_window : float, optional
        The time window (in seconds) from the event time to check for coincidences.
        Default is 10 seconds.
    skymap_contour : float, optional
        The contour level to use for the skymap overlap check.
        Default is 0.95 (i.e. the 95% contour level).
    type_limit : bool, optional
        If True, only check for events of the same type as the given notice.
        Default is True.

    Returns
    -------
    matching_events : list of int
        A list of database IDs for any matching events.
        If no matches are found, an empty list is returned.
    """
    with alert_db.session_manager() as session:
        # Get any Events from the database that have a matching event time
        query = session.query(alert_db.Event)
        query = query.filter(alert_db.Event.time >= notice.event_time - time_window * u.second)
        query = query.filter(alert_db.Event.time <= notice.event_time + time_window * u.second)
        query = query.filter(alert_db.Event.name != notice.event_name) # Don't include this event!
        if type_limit:
            query = query.filter(alert_db.Event.type == notice.event_type)
        db_events = query.all()

        # Nothing within the time window
        if len(db_events) == 0:
            return []

        # We found some, but we want to check if any of them overlap with this notice's skymap
        # We want to check both the notice skymaps and the selected tiles,
        # as either overlapping should trigger a match.
        matching_events = []
        for db_event in db_events:
            # We only care about the latest notice for each event,
            # any previous ones should have already been deleted.
            event_notice = db_event.notices[-1].gcn

            # Check if either of the notice skymaps or tilesets overlap
            skymap_overlap = get_skymap_overlap(
                notice.skymap, event_notice.skymap, contour=skymap_contour
            )
            tile_overlap = get_tile_overlap(notice, event_notice)
            if tile_overlap == 0 and skymap_overlap == 0:
                # Neither overlap, so the time must have been a coincidence
                continue

            # We have a match!
            matching_events.append(db_event.db_id)

    return matching_events

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
        notice, time_window=time_window, skymap_contour=skymap_contour
    )
    if len(coincident_events) == 0:
        # No matching events found, so nothing to do
        log.debug('No coincident events found')
        return False
    log.info(f'Found {len(coincident_events)} coincident events')

    # There are matching events, now we want to go through and check if any of them
    # are "better" than this one.
    with alert_db.session_manager() as session:
        found_better = False
        for db_event_id in coincident_events:
            # Get the event ant latest notice from the database
            db_event = session.query(alert_db.Event).filter_by(db_id=db_event_id).one()
            event_notice = db_event.notices[-1].gcn
            log.info(f'Coincident event: {db_event.name}')
            log.info(f'Latest notice: {event_notice.ivorn}')

            # Check if the notice has any targets
            if len(db_event.notices[-1].targets) == 0:
                # No targets for this notice (might have been below min_prob), so we can ignore it
                log.info('Notice has no targets defined')
                continue

            # Get the parameters
            # Yes this is duplicating what happens in find_coincident_notices(),
            # but that doesn't include logging.
            time_diff = abs(event_notice.event_time - notice.event_time)
            skymap_overlap = get_skymap_overlap(
                notice.skymap, event_notice.skymap, contour=skymap_contour
            )
            tile_overlap = get_tile_overlap(notice, event_notice)
            log.debug(f'Time difference: {time_diff.value:.1f}s')
            log.debug(f'Skymap overlap: {skymap_overlap:.0%}')
            log.debug(f'Tile overlap: {tile_overlap:.0%}')

            # Now we want to decide which notice is best to observe.
            # It's not actually that obvious. We could compare the size of the skymaps or
            # the number of tiles, but thanks to different selection limits you could have a
            # case where a smaller skymap had more tiles added to the ObsDB.
            # Easiest is just to go with the skymap area for which event better localised and
            # therefore closer to the "real" position.
            old_area = event_notice.skymap.get_contour_area(skymap_contour)
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
