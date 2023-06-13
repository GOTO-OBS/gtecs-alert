"""Slack messaging tools."""

import os

from astroplan import AltitudeConstraint, AtNightConstraint, Observer, is_observable

import astropy.units as u
from astropy.time import Time

from gtecs.common.slack import send_message
from gtecs.obs import database as obs_db

import ligo.skymap.plot  # noqa: F401  (for extra projections)

from matplotlib import pyplot as plt

import numpy as np

from . import database as alert_db
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

    Other parameters are passed to `gtecs.common.slack.send_message`.

    """
    if channel is None:
        channel = params.SLACK_DEFAULT_CHANNEL

    if params.ENABLE_SLACK:
        # Use the common function
        return send_message(text, channel, params.SLACK_BOT_TOKEN, *args, **kwargs)
    else:
        print('Slack Message:', text)


def send_notice_report(notice, slack_channel=None, enable_forwarding=True, time=None):
    """Send a message to Slack with the notice details and skymap."""
    if time is None:
        time = Time.now()

    msg = f'*{notice.source} notice:* {notice.ivorn}\n'

    # Add basic notice details
    msg += f'Notice type: {notice.packet_type}\n'
    msg += f'Notice time: {notice.time.iso}'
    msg += f' _({(time - notice.time).to(u.hour).value:.1f}h ago)_\n'

    if notice.role != 'observation':
        msg += f'*NOTE: THIS IS A {notice.role.upper()} EVENT*\n'

    # Make sure we have the skymap downloaded
    notice.get_skymap()

    # Get event-specific details from the notice class
    msg += '\n'
    msg += notice.slack_details

    # Get strategy details (a short version compared to the full notice)
    msg += '\n'
    msg += f'Observing strategy: `{notice.strategy}`\n'
    if notice.strategy_dict is not None:
        cadences = ','.join(f'`{cadence}`' for cadence in notice.strategy_dict['cadence'])
        msg += f'Cadence{"s" if cadences.count(",") > 0 else ""}: {cadences}\n'
        msg += f'Constraints: `{notice.strategy_dict["constraints"]}`\n'
        msg += f'Exposure sets: `{notice.strategy_dict["exposure_sets"]}`\n'
        stop_time = max(d['stop_time'] for d in notice.strategy_dict['cadence_dict'])
        msg += f'Valid until: {stop_time.iso}'
        if stop_time < time:
            msg += f' _(expired {(time - stop_time).to("day").value:.1f} days ago)_\n'
        else:
            msg += '\n'

    # Create a skymap plot to attach to the message (if there is one)
    filepath = None
    if notice.skymap is not None:
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

    # Forward to another channel if requested
    if enable_forwarding:
        forward_message = f'*<{message_link}|New {notice.event_type} notice received>*'
        if hasattr(notice, 'short_details'):
            forward_message += '\n'
            forward_message += notice.short_details

        if notice.event_type == 'GW' and params.SLACK_GW_FORWARD_CHANNEL is not None:
            # TODO: Only initial alerts?
            send_slack_msg(forward_message, channel=params.SLACK_GW_FORWARD_CHANNEL)
        if notice.event_type == 'GRB' and params.SLACK_GRB_FORWARD_CHANNEL is not None:
            send_slack_msg(forward_message, channel=params.SLACK_GRB_FORWARD_CHANNEL)
        if 'wakeup_alert' in notice.strategy_dict and params.SLACK_WAKEUP_CHANNEL is not None:
            forward_message = '*WAKEUP ALERT:* ' + forward_message
            send_slack_msg(forward_message, channel=params.SLACK_WAKEUP_CHANNEL)

    return message_link


def send_strategy_report(notice, slack_channel=None):
    """Send a message to Slack with the observation strategy details."""
    if notice.role == 'observation':
        s = f'*Strategy for event {notice.event_name}*\n'
    else:
        s = f'*Strategy for {notice.role} event {notice.event_name}*\n'

    msg += f'Observing strategy: `{notice.strategy}`\n'
    if notice.strategy_dict is None:
        s += 'ERROR: No strategy details given\n'
        return send_slack_msg(s, channel=slack_channel)

    # Basic strategy
    s += f'Rank: {notice.strategy_dict["rank"]}\n'

    # Cadence
    for i, cadence in enumerate(notice.strategy_dict['cadence']):
        cadence_dict = notice.strategy_dict['cadence_dict'][i]
        s += f'Cadence {i + 1}: `{cadence}`\n'
        s += f'- Number of visits: {cadence_dict["num_todo"]}\n'
        s += f'- Time between visits (hours): {cadence_dict["wait_hours"]}\n'
        s += f'- Start time: {cadence_dict["start_time"].iso}\n'
        s += f'- Stop time: {cadence_dict["stop_time"].iso}\n'

    # Constraints
    s += f'Constraints: `{notice.strategy_dict["constraints"]}`\n'
    s += f'- Min Alt: {notice.strategy_dict["constraints_dict"]["min_alt"]}\n'
    s += f'- Max Sun Alt: {notice.strategy_dict["constraints_dict"]["max_sunalt"]}\n'
    s += f'- Min Moon Sep: {notice.strategy_dict["constraints_dict"]["min_moonsep"]}\n'
    s += f'- Max Moon Phase: {notice.strategy_dict["constraints_dict"]["max_moon"]}\n'

    # Exposure Sets
    s += f'ExposureSets: `{notice.strategy_dict["exposure_sets"]}`\n'
    for expset in notice.strategy_dict['exposure_sets_dict']:
        s += f'- NumExp: {expset["num_exp"]:.0f}'
        s += f'  Filter: {expset["filt"]}'
        s += f'  ExpTime: {expset["exptime"]:.1f}s\n'

    # Tiling
    s += f'Tile number limit: {notice.strategy_dict["tile_limit"]}\n'
    s += f'Tile probability limit: {notice.strategy_dict["prob_limit"]:.1%}\n'

    # Send the message
    return send_slack_msg(s, channel=slack_channel)


def send_observing_report(notice, slack_channel=None, time=None):
    """Send a message to Slack with details of the observing details and visibility."""
    if time is None:
        time = Time.now()

    if notice.strategy == 'IGNORE':
        # No reason to send a message
        # (NB Retractions still check the database that the pointings have been removed)
        return

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
        pending = [p
                   for survey in db_event.surveys
                   for p in survey.pointings
                   if p.status_at_time(time + 1 * u.s) not in ['deleted', 'expired', 'completed']]
        msg += f'- Event has {len(pending)} pending pointings\n'

        # Look at the Survey this Notice is linked to (if any)
        db_survey = db_notice.survey

        if db_survey is None:
            # It could be a retraction
            if notice.strategy == 'RETRACTION':
                if len(pending) == 0:
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
        all_tiles = grid.get_table()
        if (notice.strategy_dict['prob_limit'] > 0 and
                max(all_tiles['prob']) < notice.strategy_dict['prob_limit']):
            msg += '- No tiles passed the probability limit '
            msg += f'({notice.strategy_dict["prob_limit"]:.1%}, '
            msg += f'highest had {max(all_tiles["prob"]):.1%})\n'
        else:
            # Uh-oh, something went wrong when inserting?
            msg += '- *ERROR: No targets found in database*\n'
        return send_slack_msg(msg, channel=slack_channel)

    total_prob = grid.get_probability(survey_tiles)
    msg += f'Total probability in survey tiles: {total_prob:.1%}\n'

    # Create visibility plot
    fig = plt.figure(figsize=(9, 4 * len(sites)), dpi=120, facecolor='white', tight_layout=True)

    # Find visibility constraints
    min_alt = float(notice.strategy_dict['constraints_dict']['min_alt']) * u.deg
    max_sunalt = float(notice.strategy_dict['constraints_dict']['max_sunalt']) * u.deg
    alt_constraint = AltitudeConstraint(min=min_alt)
    night_constraint = AtNightConstraint(max_solar_altitude=max_sunalt)
    constraints = [alt_constraint, night_constraint]
    start_time = min(d['start_time'] for d in notice.strategy_dict['cadence_dict'])
    stop_time = max(d['stop_time'] for d in notice.strategy_dict['cadence_dict'])

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

    # Send the message with the plot attached
    return send_slack_msg(msg, filepath=filepath, channel=slack_channel)
