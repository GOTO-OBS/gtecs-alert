"""Slack messaging tools."""

import os

from astroplan import AltitudeConstraint, AtNightConstraint, Observer, is_observable

import astropy.units as u
from astropy.time import Time

from gtecs.common.slack import send_message
from gtecs.obs import database as obs_db

import ligo.skymap.plot  # noqa: F401  (for extra projections)

import matplotlib
from matplotlib import pyplot as plt

import numpy as np

from . import database as alert_db
from . import params
from .notices import GWNotice


def send_slack_msg(text, channel=None, *args, **kwargs):
    """Send a message to Slack.

    Parameters
    ----------
    text : string
        The message text.
    channel : string, optional
        The channel to post the message to.
        If None, defaults to `params.SLACK_DEFAULT_CHANNEL`.

    Other parameters are passed to `gtecs.common.slack.send_message`.

    """
    if channel is None:
        channel = params.SLACK_DEFAULT_CHANNEL

    if params.ENABLE_SLACK:
        # Use the common function
        return send_message(text, channel, params.SLACK_BOT_TOKEN, *args, **kwargs)
    else:
        print('Slack Message:', text)


def send_notice_report(notice, time=None):
    """Send a message to Slack with the notice details and skymap."""
    if time is None:
        time = Time.now()

    # Get the correct slack channel
    slack_channel = get_slack_channel(notice)

    msg = f'*{notice.source} notice:* {notice.ivorn}\n'

    # Add basic notice details
    msg += f'Notice type: {notice.type}\n'
    msg += f'Notice time: {notice.time.iso}'
    msg += f' _({(time - notice.time).to(u.hour).value:.1f}h ago)_\n'

    if notice.role != 'observation':
        msg += f'*NOTE: THIS IS A {notice.role.upper()} EVENT*\n'
    msg += '\n'

    # Make sure we have the skymap downloaded
    notice.get_skymap()

    # Basic event details (retractions don't have an event_time)
    msg += f'Event: {notice.event_name}\n'
    if notice.event_time is not None:
        msg += f'Detection time: {notice.event_time.iso}'
        msg += f' _({(time - notice.event_time).to(u.hour).value:.1f}h ago)_\n'

    # Add event-specific details from the notice class
    msg += notice.slack_details

    # Get strategy details (a short version compared to the full notice)
    msg += '\n'
    msg += f'Observing strategy: `{notice.strategy}`\n'
    if notice.strategy_dict is not None:
        msg += 'Cadence: '
        if isinstance(notice.strategy_dict['cadence'], dict):
            cadences = [notice.strategy_dict['cadence']]
        else:
            cadences = notice.strategy_dict['cadence']
        for i, cadence in enumerate(cadences):
            if 'delay_hours' in notice.strategy_dict:
                msg += f'wait for {cadence["delay_hours"]}h; then '
            msg += f'{cadence["num_todo"]} observations'
            if cadence['num_todo'] > 1:
                if not isinstance(cadence['wait_hours'], list):
                    waits = [cadence['wait_hours']]
                else:
                    waits = cadence['wait_hours']
                waits = [f'{waits[i % len(waits)]}h' for i in range(cadence['num_todo'] - 1)]
                if len(waits) > 3:
                    # limit to showing 3 observations
                    waits = "/".join(waits[:3]) + '/...'
                else:
                    waits = "/".join(waits)
                msg += f', delay{"s" if cadence["num_todo"] > 2 else ""} of {waits}'
            msg += f', valid for {notice.strategy_dict["valid_hours"]}h'
            if i != len(cadences) - 1:
                msg += '; then '
        msg += '\n'
        msg += 'Constraints: '
        msg += f'alt>{notice.strategy_dict["constraints"]["min_alt"]}°, '
        msg += f'sun<{notice.strategy_dict["constraints"]["max_sunalt"]}°, '
        msg += f'moon≤{notice.strategy_dict["constraints"]["max_moon"]}, '
        msg += f'moonsep>{notice.strategy_dict["constraints"]["min_moonsep"]}°\n'
        msg += 'Exposure sets: '
        for i, exposure_set in enumerate(notice.strategy_dict['exposure_sets']):
            msg += f'{exposure_set["num_exp"]}x{exposure_set["exptime"]}{exposure_set["filt"]}'
            if i != len(notice.strategy_dict['exposure_sets']) - 1:
                msg += ' + '
        msg += '\n'
        stop_time = max(c['stop_time'] for c in cadences)
        msg += f'Valid until: {stop_time.iso}'
        if stop_time < time:
            msg += f' _(expired {(time - stop_time).to("day").value:.1f} days ago)_\n'
        else:
            msg += '\n'

    # Create a skymap plot to attach to the message (if there is one)
    filepath = None
    if notice.skymap is not None:
        matplotlib.use('agg')  # Use the agg backend for plotting, so we don't need a display

        plt.figure(figsize=(8, 4), dpi=120, facecolor='white', tight_layout=True)
        axes = plt.axes(projection='astro hours mollweide')
        axes.grid()

        # Plot the skymap data and contours
        notice.skymap.plot_data(axes, rasterize=False, cmap='cylon', cbar=False)
        notice.skymap.plot_contours(axes, levels=[0.5, 0.9],
                                    colors='black', linewidths=0.5, linestyles=['dashed', 'solid'],
                                    zorder=1.2,
                                    )

        # For small areas, add a marker
        if notice.position and notice.skymap.get_contour_area(0.9) < 10:
            axes.scatter(
                notice.position.ra.value, notice.position.dec.value,
                transform=axes.get_transform('world'),
                s=99, c='tab:blue', marker='*',
                zorder=9,
            )
            axes.text(
                notice.position.ra.value, notice.position.dec.value,
                notice.position.to_string('hmsdms').replace(' ', '\n') + '\n',
                transform=axes.get_transform('world'),
                ha='center', va='bottom',
                size='x-small',
                zorder=12,
            )

        # Add text
        axes.set_title(f'Skymap for {notice.event_type} event {notice.event_name}', y=1.06)
        axes.text(0.5, 1.03, f'{notice.ivorn}', fontsize=8, ha='center', transform=axes.transAxes)
        axes.text(-0.03, -0.1, f'Detection time: {notice.event_time.strftime("%Y-%m-%d %H:%M:%S")}',
                  ha='left', va='bottom', transform=axes.transAxes)
        if notice.skymap.get_contour_area(0.9) < 10:
            text = f'50% area: {notice.skymap.get_contour_area(0.5):.2f} deg²\n'
            text += f'90% area: {notice.skymap.get_contour_area(0.9):.2f} deg²'
        else:
            text = f'50% area: {notice.skymap.get_contour_area(0.5):.0f} deg²\n'
            text += f'90% area: {notice.skymap.get_contour_area(0.9):.0f} deg²'
        axes.text(0.8, -0.1, text, ha='left', va='bottom', transform=axes.transAxes)

        # Save
        direc = os.path.join(params.FILE_PATH, 'plots')
        if not os.path.exists(direc):
            os.makedirs(direc)
        filepath = os.path.join(direc, notice.event_name + '_skymap.png')
        plt.savefig(filepath)
        plt.close(plt.gcf())

    # Send the message
    message_link = send_slack_msg(msg, filepath=filepath, channel=slack_channel, return_link=True)

    # If not sent to the default channel, send a copy there too
    if slack_channel != params.SLACK_DEFAULT_CHANNEL:
        forward_message = f'<{message_link}|Notice details>'
        send_slack_msg(forward_message, channel=params.SLACK_DEFAULT_CHANNEL)

    # Forward to the wakeup channel if requested
    if (notice.strategy_dict is not None and 'wakeup_alert' in notice.strategy_dict and
            params.SLACK_WAKEUP_CHANNEL is not None):
        forward_message = f'*WAKEUP ALERT: <{message_link}|New notice received>*'
        if hasattr(notice, 'short_details'):
            forward_message += '\n'
            forward_message += notice.short_details
        send_slack_msg(forward_message, channel=params.SLACK_WAKEUP_CHANNEL)


def get_slack_channel(notice):
    """Get the correct slack channel for a notice."""
    if notice.strategy == 'RETRACTION':
        # We want the retraction to go to the same channel as the original notice,
        # which might be the ignored channel.
        # So we need to get the previous alert from GraceDB.
        try:
            n = int(notice.ivorn.split('-')[1])
            prev_notice = GWNotice.from_gracedb(notice.event_id, n - 1)
            prev_notice.get_skymap()
            return get_slack_channel(prev_notice)
        except Exception:
            # Fall back to the other options
            pass  # TODO Should be raise, or at least some logged warning...
    elif notice.strategy == 'IGNORE' and params.SLACK_IGNORED_CHANNEL is not None:
        # Ignored notices are still useful to log on Slack
        return params.SLACK_IGNORED_CHANNEL

    if (notice.event_type in params.SLACK_EVENT_CHANNELS and
            params.SLACK_EVENT_CHANNELS[notice.event_type] is not None):
        # Send to the specific event channel if it exists
        return params.SLACK_EVENT_CHANNELS[notice.event_type]
    else:
        # Just send to the default channel
        return params.SLACK_DEFAULT_CHANNEL


def send_observing_report(notice, time=None):
    """Send a message to Slack with details of the observing details and visibility."""
    if time is None:
        time = Time.now()

    if notice.strategy == 'IGNORE':
        # No reason to send a message
        # (NB Retractions still check the database that the pointings have been removed)
        return

    # Get the correct slack channel
    slack_channel = get_slack_channel(notice)

    msg = f'*{notice.source} notice:* {notice.ivorn}\n'

    # Get info from the alert database
    with alert_db.session_manager() as session:
        # Query the Notice table for the matching entry
        query = session.query(alert_db.Notice).filter(alert_db.Notice.ivorn == notice.ivorn)
        db_notice = query.one_or_none()
        if db_notice is None:
            msg += '*ERROR: No matching entry found in database*\n'
            return send_slack_msg(msg, channel=slack_channel)
        msg += f'Notice added to database (ID={db_notice.db_id})\n'

        # Look at the Event this Notice is for
        db_event = db_notice.event
        if db_event is None:
            msg += '*ERROR: No matching event found in database*\n'
            return send_slack_msg(msg, channel=slack_channel)
        msg += f'Notice linked to Event `{db_event.name}` (ID={db_event.db_id})\n'
        msg += f'- Event is linked to {len(db_event.notices)} notices'
        msg += f' and {len(db_event.surveys)} surveys\n'
        status_time = time + 1 * u.s
        scheduled = [
            t for survey in db_event.surveys for t in survey.targets
            if t.scheduled_at_time(status_time)
        ]
        msg += f'- Event has {len(scheduled)} scheduled targets'
        running = [
            p for survey in db_event.surveys for p in survey.pointings
            if p.status_at_time(status_time) == 'running'
        ]
        if len(running) > 0:
            msg += f' ({len(running)} are currently being observed)'
        msg += '\n'

        # Look at the Survey this Notice is linked to (if any)
        db_survey = db_notice.survey

        if db_survey is None:
            # It could be a retraction
            if notice.strategy == 'RETRACTION':
                # Make sure there are no targets still scheduled (running is fine)
                if len(scheduled) == 0 or len(scheduled) == len(running):
                    msg += 'Event has been successfully retracted\n'
                else:
                    msg += '*ERROR: Retraction failed to remove pending pointings*\n'
                return send_slack_msg(msg, channel=slack_channel)
            else:
                # Uh-oh, something went wrong when inserting?
                msg += '*ERROR: No survey found in database*\n'
                return send_slack_msg(msg, channel=slack_channel)

        # We have a Survey in the database, but it might be an old one
        if len(db_survey.notices) > 1 and db_survey.notices[0] != db_notice:
            # This is an old Survey created for a previous Notice
            msg += f'Notice linked to existing Survey `{db_survey.name}` (ID={db_survey.db_id})\n'
            msg += f'- Survey created from notice {db_survey.notices[0].ivorn}\n'
            msg += '- Event skymap and strategy are unchanged\n'
            return send_slack_msg(msg, channel=slack_channel)

        msg += f'Notice linked to new Survey `{db_survey.name}` (ID={db_survey.db_id})\n'
        msg += f'- Survey contains {len(db_survey.targets)} targets\n'

        # Save info from the database here, so we can close the connection
        survey_name = db_survey.name
        survey_tiles = np.array([target.grid_tile.name for target in db_survey.targets])

    # Get grid and site info from the obsdb
    with obs_db.session_manager() as session:
        db_grid = obs_db.get_current_grid(session)
        grid = db_grid.skygrid

        db_sites = session.query(obs_db.Site).all()
        sites = [site.location for site in db_sites]
        site_names = [site.name for site in db_sites]

    # Now we want to calculate the current visibility of the survey at each site
    # We're going to have to re-apply the skymap to the grid to get the tile probabilities
    grid.apply_skymap(notice.skymap)

    if len(survey_tiles) == 0:
        # This might be because no tiles passed the filter
        if (notice.strategy_dict['min_tile_prob'] > 0 and
                max(grid.prob) < notice.strategy_dict['min_tile_prob']):
            msg += '- No tiles passed the probability limit '
            msg += f'({notice.strategy_dict["min_tile_prob"]:.1%}, '
            msg += f'highest had {max(grid.prob):.1%})\n'
        else:
            # Uh-oh, something went wrong when inserting?
            msg += '- *ERROR: No targets found in database*\n'
        return send_slack_msg(msg, channel=slack_channel)

    total_prob = grid.get_probability(survey_tiles)
    msg += f'Total probability in survey tiles: {total_prob:.1%}\n'

    # Create visibility plot
    matplotlib.use('agg')  # Use the agg backend for plotting, so we don't need a display
    fig = plt.figure(figsize=(9, 4 * len(sites)), dpi=120, facecolor='white', tight_layout=True)

    # Find visibility constraints
    min_alt = float(notice.strategy_dict['constraints']['min_alt']) * u.deg
    max_sunalt = float(notice.strategy_dict['constraints']['max_sunalt']) * u.deg
    alt_constraint = AltitudeConstraint(min=min_alt)
    night_constraint = AtNightConstraint(max_solar_altitude=max_sunalt)
    constraints = [alt_constraint, night_constraint]
    if isinstance(notice.strategy_dict['cadence'], dict):
        cadences = [notice.strategy_dict['cadence']]
    else:
        cadences = notice.strategy_dict['cadence']
    start_time = min(c['start_time'] for c in cadences)
    stop_time = max(c['stop_time'] for c in cadences)

    for i, site in enumerate(sites):
        observer = Observer(site)
        site_name = site_names[i]
        if site_name == 'Roque de los Muchachos, La Palma':
            site_name = 'La Palma'
        elif site_name == 'Siding Spring Observatory':
            site_name = 'Siding Spring'
        msg += f'Predicted visibility from {site_name}:\n'

        # Find which grid tiles are visible from this site
        visible_mask = is_observable(constraints, observer, grid.coords,
                                     time_range=[start_time, stop_time])
        visible_tiles = set(np.array(grid.tilenames)[visible_mask])

        # Now find which skymap tiles are visible
        visible_survey_tiles = {t for t in survey_tiles if t in visible_tiles}
        msg += '- Tiles visible during valid period:'
        msg += f' {len(visible_survey_tiles)}/{len(survey_tiles)}\n'

        # Find the probability for all tiles and those visible
        visible_prob = grid.get_probability(visible_survey_tiles)
        msg += f'- Probability in visible tiles: {visible_prob:.1%}\n'

        # Add to plot
        axes = plt.subplot(11 + len(sites) * 100 + i, projection='astro hours mollweide')

        # Plot the tiles coloured by probability
        t = grid.plot_tiles(
            axes, array=grid.probs,
            ec='none', alpha=0.8, cmap='cylon',
            zorder=1,
        )
        t.set_clim(vmin=0, vmax=max(grid.probs))
        grid.plot_tiles(axes, fc='none', ec='0.3', lw=0.1, zorder=1.2)

        # Add the colorbar, formatting as a percentage
        fig.colorbar(
            t, ax=axes, fraction=0.02, pad=0.05,
            # label='Tile contained probability',
            format=lambda x, _: f'{x:.1%}' if max(grid.probs) < 0.1 else f'{x:.0%}',
        )

        # Add contours
        notice.skymap.plot_contours(
            axes, levels=[0.5, 0.9],
            colors='black', linewidths=0.5, linestyles=['dashed', 'solid'],
            zorder=1.15,
        )

        # Overcast non-visible tiles
        alphas = [0 if t in visible_tiles else 0.3 for t in grid.tilenames]
        grid.plot_tiles(axes, fc='0.5', ec='none', alpha=alphas, zorder=1.1)

        # Add the tile outlines coloured by visibility
        ec = ['tab:blue' if tilename in visible_survey_tiles
              else 'tab:red' if tilename in survey_tiles
              else 'none'
              for tilename in grid.tilenames]
        grid.plot_tiles(axes, fc='none', ec=ec, lw=1, zorder=1.21)

        # Add text
        if i == 0:
            axes.set_title(f'Tiling for survey {survey_name}', y=1.06)
            text = f'Showing site visibility for {(stop_time-start_time).to(u.h).value:.1f}h '
            text += f'starting {start_time.strftime("%Y-%m-%d %H:%M:%S")}'
            axes.text(0.5, 1.03, text, fontsize=8, ha='center', transform=axes.transAxes)
        axes.text(-0.03, -0.06, f'Site: {site_name}',
                  ha='left', va='bottom', transform=axes.transAxes)
        text = f'Visible tiles: {len(visible_survey_tiles)}/{len(survey_tiles)}\n'
        text += f'Visible probability: {visible_prob:.1%}'
        axes.text(0.78, -0.06, text, ha='left', va='bottom', transform=axes.transAxes)

    # Save
    direc = os.path.join(params.FILE_PATH, 'plots')
    if not os.path.exists(direc):
        os.makedirs(direc)
    filepath = os.path.join(direc, notice.event_name + '_tiles.png')
    plt.savefig(filepath)
    plt.close(plt.gcf())

    # Send the message
    message_link = send_slack_msg(msg, filepath=filepath, channel=slack_channel, return_link=True)

    # If not sent to the default channel, send a copy there too
    if slack_channel != params.SLACK_DEFAULT_CHANNEL:
        forward_message = f'<{message_link}|Observing details>'
        send_slack_msg(forward_message, channel=params.SLACK_DEFAULT_CHANNEL)
