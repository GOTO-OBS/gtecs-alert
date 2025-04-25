"""Functions for detecting coincident events."""

from astropy import units as u

import numpy as np

from . import database as alert_db

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


def find_coincident_events(
        notice,
        time_window=10,
        skymap_contour=0.95,
        type_limit=True,
        return_parameters=False
    ):
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
    return_parameters : bool, optional
        If True, return the time difference, skymap overlap, and tile overlap
        for each matching event.
        Default is False.
        If False, only return the matching event IDs.

    Returns
    -------
    matched_event_ids : list of int or list of tuples
        If `return_parameters` is False, a list of database IDs for any matching events.
        If `return_parameters` is True, a list of tuples containing the database ID,
        time difference, skymap overlap, and tile overlap for each matching event.
        Each tuple is of the form (db_id, time_diff, skymap_overlap, tile_overlap).
        The time difference is in seconds, and the skymap and tile overlaps are fractions
        between 0 and 1.
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
        query = query.order_by(alert_db.Event.time)
        matched_events = query.all()

        # Nothing within the time window
        if len(matched_events) == 0:
            return []

        # We found some, but we want to check if any of them overlap with this notice's skymap
        # We want to check both the notice skymaps and the selected tiles,
        # as either overlapping should trigger a match.
        matched_event_ids = []
        for matched_event in matched_events:
            # We only care about the latest notice for each event,
            # any previous ones should have already been deleted.
            matched_notice = matched_event.notices[-1].gcn
            time_diff = (notice.event_time - matched_notice.event_time).to(u.second).value

            # Check if either of the notice skymaps or tilesets overlap
            skymap_overlap = get_skymap_overlap(
                notice.skymap, matched_notice.skymap, contour=skymap_contour
            )
            tile_overlap = get_tile_overlap(notice, matched_notice)
            if tile_overlap == 0 and skymap_overlap == 0:
                # Neither overlap, so the time must have been a coincidence
                continue

            # We have a match!
            if return_parameters:
                params = (
                    matched_event.db_id,
                    time_diff,
                    skymap_overlap,
                    tile_overlap
                )
                matched_event_ids.append(params)
            else:
                # Just append the event ID
                matched_event_ids.append(matched_event.db_id)

    return matched_event_ids
