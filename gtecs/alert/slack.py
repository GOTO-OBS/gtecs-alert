"""Slack messaging tools."""

import os

from astroplan import AltitudeConstraint, AtNightConstraint, Observer, is_observable

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from gtecs.common.slack import send_message
from gtecs.obs import database as db

import numpy as np

from . import params


def send_slack_msg(text, channel=None, *args, **kwargs):
    """Send a message to Slack.

    Parameters
    ----------
    text : string
        The message text.
    channel : string, optional
        The channel to post the message to.
        If None, defaults to `gtecs.alert.params.SLACK_DEFAULT_CHANNEL`.

    Other parameters are passed to `gtecs.common.slack.send_slack_msg`.

    """
    if channel is None:
        channel = params.SLACK_DEFAULT_CHANNEL

    if params.ENABLE_SLACK:
        # Use the common function
        send_message(text, channel, params.SLACK_BOT_TOKEN, *args, **kwargs)
    else:
        print('Slack Message:', text)


def send_event_report(event, slack_channel=None):
    """Send a message to Slack with the event details and skymap."""
    s = f'*Details for event {event.name}*\n'

    # Get list of details based on the event class
    details = event.get_details()
    s += '\n'.join(details)

    # Plot skymap (if it has one)
    filepath = None
    if event.skymap is not None:
        direc = os.path.join(params.FILE_PATH, 'plots')
        if not os.path.exists(direc):
            os.makedirs(direc)
        filepath = os.path.join(direc, event.name + '_skymap.png')
        # Plot the centre of events that have one
        # TODO: Improve the plot!
        if event.coord:
            event.skymap.plot(filename=filepath, coordinates=event.coord)
        else:
            event.skymap.plot(filename=filepath)

    # Send the message, with the skymap file attached
    send_slack_msg(s, filepath=filepath, channel=slack_channel)


def send_strategy_report(event, slack_channel=None):
    """Send a message to Slack with the event strategy details."""
    if event.strategy is None:
        return

    s = f'*Strategy for event {event.name}*\n'

    # Basic strategy
    strategy = event.get_strategy()
    s += f'Strategy: `{strategy["strategy"]}`\n'
    s += f'Rank: {strategy["rank"]}\n'

    # Cadence
    for i, cadence in enumerate(strategy['cadence']):
        cadence_dict = strategy['cadence_dict'][i]
        s += f'Cadence {i + 1}: `{cadence}`\n'
        s += f'- Number of visits: {cadence_dict["num_todo"]}\n'
        s += f'- Time between visits (hours): {cadence_dict["wait_hours"]}\n'
        s += f'- Start time: {cadence_dict["start_time"].iso}\n'
        s += f'- Stop time: {cadence_dict["stop_time"].iso}\n'

    # Constraints
    s += f'Constraints: `{strategy["constraints"]}`\n'
    s += f'- Min Alt: {strategy["constraints_dict"]["min_alt"]}\n'
    s += f'- Max Sun Alt: {strategy["constraints_dict"]["max_sunalt"]}\n'
    s += f'- Min Moon Sep: {strategy["constraints_dict"]["min_moonsep"]}\n'
    s += f'- Max Moon Phase: {strategy["constraints_dict"]["max_moon"]}\n'

    # Exposure Sets
    s += f'ExposureSets: `{strategy["exposure_sets"]}`\n'
    for expset in strategy['exposure_sets_dict']:
        s += f'- NumExp: {expset["num_exp"]:.0f}'
        s += f'  Filter: {expset["filt"]}'
        s += f'  ExpTime: {expset["exptime"]:.1f}s\n'

    # Tiling
    s += f'Tile number limit: {strategy["tile_limit"]}\n'
    s += f'Tile probability limit: {strategy["prob_limit"]:.1%}\n'

    # Send the message
    send_slack_msg(s, channel=slack_channel)


def send_database_report(event, slack_channel=None, time=None):
    """Send a message to Slack with details of the database pointings and visibility."""
    if time is None:
        time = Time.now()

    s = f'*Visibility for event {event.name}*\n'
    with db.open_session() as session:
        # Query Event table
        db_event = session.query(db.Event).filter(db.Event.name == event.name).one_or_none()
        if db_event is None:
            # Uh-oh, send a warning message
            s += '*ERROR: No matching entry found in database*\n'
            send_slack_msg(s, channel=slack_channel)
            return
        s += f'Number of surveys found for this event: {len(db_event.surveys)}\n'

        # Send different alerts for retractions
        if event.strategy is None:
            # Check that all pointings for this event have been deleted
            pending = [p for p in db_event.pointings
                       if p.status_at_time(time) not in ['deleted', 'expired', 'completed']]
            s += f'Number of pending pointings found for this event: {len(pending)}\n'
            if len(pending) == 0:
                s += '- Previous targets removed successfully\n'
            else:
                s += '- *ERROR: Retraction failed to remove previous targets*\n'
            # That's it for retractions
            send_slack_msg(s, channel=slack_channel)
            return

        # We are considering only the latest survey
        db_survey = db_event.surveys[-1]
        s += f'Latest survey: {db_survey.name}\n'
        s += f'Number of targets for this survey: {len(db_survey.targets)}\n'

        # Check non-retractions with no targets
        if len(db_survey.targets) == 0:
            # This might be because no tiles passed the filter
            if (event.strategy['prob_limit'] > 0 and
                    max(event.full_table['prob']) < event.strategy['prob_limit']):
                s += '- No tiles passed the probability limit '
                s += f'({event.strategy["prob_limit"]:.1%}, '
                s += f'highest had {max(event.full_table["prob"]):.1%})\n'
            else:
                # Uh-oh, something went wrong when inserting?
                s += '- *ERROR: No targets found in database*\n'
            send_slack_msg(s, channel=slack_channel)
            return

        # We have at least 1 target, so we can consider the grid visibility
        coords = SkyCoord([target.ra for target in db_survey.targets],
                          [target.dec for target in db_survey.targets],
                          unit='deg')
        tiles = np.array([target.grid_tile.name for target in db_survey.targets])

        # Create visibility plots
        filepath = None
        for site in ['La Palma']:  # TODO: should get sites from db
            observer = Observer.at_site(site.lower().replace(' ', ''))
            s += f'Predicted visibility from {site}:\n'

            # Find visibility constraints
            min_alt = float(event.strategy['constraints_dict']['min_alt']) * u.deg
            max_sunalt = float(event.strategy['constraints_dict']['max_sunalt']) * u.deg
            alt_constraint = AltitudeConstraint(min=min_alt)
            night_constraint = AtNightConstraint(max_solar_altitude=max_sunalt)
            constraints = [alt_constraint, night_constraint]
            start_time = min(d['start_time'] for d in event.strategy['cadence_dict'])
            stop_time = max(d['stop_time'] for d in event.strategy['cadence_dict'])
            s += '- Valid dates:'
            s += f' {start_time.datetime.strftime("%Y-%m-%d")} to'
            s += f' {stop_time.datetime.strftime("%Y-%m-%d")}'
            if stop_time < Time.now():
                # The pointings will have expired
                delta = Time.now() - stop_time
                s += f' _(expired {delta.to("day").value:.1f} days ago)_\n'
            else:
                s += '\n'

            # Find which event tiles are visible during the valid period
            visible_mask = is_observable(constraints, observer, coords,
                                         time_range=[start_time, stop_time])
            event_tiles = sorted(set(tiles))
            event_tiles_visible = sorted(set(tiles[visible_mask]))
            s += '- Tiles visible during valid period:'
            s += f' {len(event_tiles_visible)}/{len(event_tiles)}\n'
            event_tiles_not_visible = sorted(set(tiles[np.invert(visible_mask)]))

            # Find the probability for all tiles and those visible
            total_prob = event.grid.get_probability(event_tiles)
            s += f'- Total probability in all tiles: {total_prob:.1%}\n'
            visible_prob = event.grid.get_probability(event_tiles_visible)
            s += f'- Probability in visible tiles: {visible_prob:.1%}\n'

            # Also get which grid tiles are visible
            grid_visible_mask = is_observable(constraints, observer, event.grid.coords,
                                              time_range=[start_time, stop_time])
            grid_tiles = np.array(event.grid.tilenames)
            grid_tiles_not_visible = grid_tiles[np.invert(grid_visible_mask)]

            # Create a plot of the tiles, showing visibility tonight
            # TODO: multiple sites? Need multiple plots or one combined?
            # TODO: this could be much nicer!
            filename = event.name + '_tiles.png'
            filepath = os.path.join(params.FILE_PATH, filename)
            event.grid.plot(filename=filepath,
                            plot_skymap=True,
                            highlight=[event_tiles_visible, event_tiles_not_visible],
                            highlight_color=['blue', 'red'],
                            color={tilename: '0.5' for tilename in grid_tiles_not_visible},
                            )

        # Send the message with the plot attached
        send_slack_msg(s, filepath=filepath, channel=slack_channel)
