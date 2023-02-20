"""Slack messaging tools."""

import os

from astroplan import AltitudeConstraint, AtNightConstraint, Observer, is_observable

import astropy.units as u
from astropy.time import Time

from gtecs.common.slack import send_message
from gtecs.obs import database as db

import ligo.skymap.plot  # noqa: F401  (for extra projections)

from matplotlib import pyplot as plt

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

    filepath = None
    if event.skymap is not None:
        # Create skymap plot
        plt.figure(figsize=(8, 4), dpi=120, facecolor='white', tight_layout=True)
        axes = plt.axes(projection='astro hours mollweide')
        axes.grid()

        # Plot the skymap data and contours
        event.skymap.healpix.plot(axes, rasterize=False, cmap='cylon', cbar=False)
        axes.contour_hpx(
            event.skymap.contours / max(event.skymap.contours),
            nested=event.skymap.is_nested,
            levels=[0.5, 0.9],
            colors='black', linewidths=0.5, linestyles=['dashed', 'solid'],
            zorder=1.2,
        )

        # For small areas, add a marker
        if event.coord and event.skymap.get_contour_area(0.9) < 10:
            axes.scatter(
                event.coord.ra.value, event.coord.dec.value,
                transform=axes.get_transform('world'),
                s=99, c='tab:blue', marker='*',
                zorder=9,
            )
            axes.text(
                event.coord.ra.value, event.coord.dec.value,
                event.coord.to_string('hmsdms').replace(' ', '\n') + '\n',
                transform=axes.get_transform('world'),
                ha='center', va='bottom',
                size='x-small',
                zorder=12,
            )

        # Add text
        axes.set_title(f'Skymap for {event.type} event {event.name}', y=1.06)
        axes.text(0.5, 1.03, f'{event.ivorn}', fontsize=8, ha='center', transform=axes.transAxes)
        axes.text(-0.03, -0.1, f'Detection time: {event.time.strftime("%Y-%m-%d %H:%M:%S")}',
                  ha='left', va='bottom', transform=axes.transAxes)
        if event.skymap.get_contour_area(0.9) < 10:
            text = f'50% area: {event.skymap.get_contour_area(0.5):.2f} deg²\n'
            text += f'90% area: {event.skymap.get_contour_area(0.9):.2f} deg²'
        else:
            text = f'50% area: {event.skymap.get_contour_area(0.5):.0f} deg²\n'
            text += f'90% area: {event.skymap.get_contour_area(0.9):.0f} deg²'
        axes.text(0.8, -0.1, text, ha='left', va='bottom', transform=axes.transAxes)

        # Save
        direc = os.path.join(params.FILE_PATH, 'plots')
        if not os.path.exists(direc):
            os.makedirs(direc)
        filepath = os.path.join(direc, event.name + '_skymap.png')
        plt.savefig(filepath)
        plt.close(plt.gcf())

    # Send the message, with the skymap file attached (if it has one)
    send_slack_msg(s, filepath=filepath, channel=slack_channel)


def send_strategy_report(event, slack_channel=None):
    """Send a message to Slack with the event strategy details."""
    s = f'*Strategy for event {event.name}*\n'

    if event.strategy is None:
        # This is a retraction
        s += '*ERROR: No strategy defined*\n'
        send_slack_msg(s, channel=slack_channel)
        return

    # Basic strategy
    s += f'Strategy: `{event.strategy_dict["strategy"]}`\n'
    s += f'Rank: {event.strategy_dict["rank"]}\n'

    # Cadence
    for i, cadence in enumerate(event.strategy_dict['cadence']):
        cadence_dict = event.strategy_dict['cadence_dict'][i]
        s += f'Cadence {i + 1}: `{cadence}`\n'
        s += f'- Number of visits: {cadence_dict["num_todo"]}\n'
        s += f'- Time between visits (hours): {cadence_dict["wait_hours"]}\n'
        s += f'- Start time: {cadence_dict["start_time"].iso}\n'
        s += f'- Stop time: {cadence_dict["stop_time"].iso}\n'

    # Constraints
    s += f'Constraints: `{event.strategy_dict["constraints"]}`\n'
    s += f'- Min Alt: {event.strategy_dict["constraints_dict"]["min_alt"]}\n'
    s += f'- Max Sun Alt: {event.strategy_dict["constraints_dict"]["max_sunalt"]}\n'
    s += f'- Min Moon Sep: {event.strategy_dict["constraints_dict"]["min_moonsep"]}\n'
    s += f'- Max Moon Phase: {event.strategy_dict["constraints_dict"]["max_moon"]}\n'

    # Exposure Sets
    s += f'ExposureSets: `{event.strategy_dict["exposure_sets"]}`\n'
    for expset in event.strategy_dict['exposure_sets_dict']:
        s += f'- NumExp: {expset["num_exp"]:.0f}'
        s += f'  Filter: {expset["filt"]}'
        s += f'  ExpTime: {expset["exptime"]:.1f}s\n'

    # Tiling
    s += f'Tile number limit: {event.strategy_dict["tile_limit"]}\n'
    s += f'Tile probability limit: {event.strategy_dict["prob_limit"]:.1%}\n'

    # Send the message
    send_slack_msg(s, channel=slack_channel)


def send_database_report(event, grid, slack_channel=None, time=None):
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
            all_tiles = grid.get_table()
            if (event.strategy_dict['prob_limit'] > 0 and
                    max(all_tiles['prob']) < event.strategy_dict['prob_limit']):
                s += '- No tiles passed the probability limit '
                s += f'({event.strategy_dict["prob_limit"]:.1%}, '
                s += f'highest had {max(all_tiles["prob"]):.1%})\n'
            else:
                # Uh-oh, something went wrong when inserting?
                s += '- *ERROR: No targets found in database*\n'
            send_slack_msg(s, channel=slack_channel)
            return

        # We have at least 1 target, so we can consider the grid visibility
        # Get info from the database here, so we can close the connection
        survey_name = db_survey.name
        event_tiles = np.array([target.grid_tile.name for target in db_survey.targets])
        db_sites = session.query(db.Site).all()
        sites = [site.location for site in db_sites]
        site_names = [site.name for site in db_sites]

    # Find visibility constraints
    min_alt = float(event.strategy_dict['constraints_dict']['min_alt']) * u.deg
    max_sunalt = float(event.strategy_dict['constraints_dict']['max_sunalt']) * u.deg
    alt_constraint = AltitudeConstraint(min=min_alt)
    night_constraint = AtNightConstraint(max_solar_altitude=max_sunalt)
    constraints = [alt_constraint, night_constraint]
    start_time = min(d['start_time'] for d in event.strategy_dict['cadence_dict'])
    stop_time = max(d['stop_time'] for d in event.strategy_dict['cadence_dict'])
    s += 'Valid dates:'
    s += f' {start_time.datetime.strftime("%Y-%m-%d")} to'
    s += f' {stop_time.datetime.strftime("%Y-%m-%d")}'
    if stop_time < Time.now():
        # The pointings will have expired
        delta = Time.now() - stop_time
        s += f' _(expired {delta.to("day").value:.1f} days ago)_\n'
    else:
        s += '\n'

    total_prob = grid.get_probability(event_tiles)
    s += f'Total probability in all tiles: {total_prob:.1%}\n'

    # Create visibility plot
    fig = plt.figure(figsize=(9, 4 * len(sites)), dpi=120, facecolor='white', tight_layout=True)

    for i, site in enumerate(sites):
        observer = Observer(site)
        site_name = site_names[i]
        if site_name == 'Roque de los Muchachos, La Palma':
            site_name = 'La Palma'
        elif site_name == 'Siding Spring Observatory':
            site_name = 'Siding Spring'
        s += f'Predicted visibility from {site_name}:\n'

        # Find which grid tiles are visible from this site
        visible_mask = is_observable(constraints, observer, grid.coords,
                                     time_range=[start_time, stop_time])
        grid_tiles = np.array(grid.tilenames)
        grid_tiles_vis = set(grid_tiles[visible_mask])

        # Now find which event tiles are visible
        event_tiles_vis = set([t for t in event_tiles if t in grid_tiles_vis])
        s += '- Tiles visible during valid period:'
        s += f' {len(event_tiles_vis)}/{len(event_tiles)}\n'

        # Find the probability for all tiles and those visible
        visible_prob = grid.get_probability(event_tiles_vis)
        s += f'- Probability in visible tiles: {visible_prob:.1%}\n'

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
        axes.contour_hpx(
            event.skymap.contours / max(event.skymap.contours),
            nested=event.skymap.is_nested,
            levels=[0.5, 0.9],
            colors='black', linewidths=0.5, linestyles=['dashed', 'solid'],
            zorder=1.15,
        )

        # Overcast non-visible tiles
        alphas = [0 if t in grid_tiles_vis else 0.3 for t in grid_tiles]
        grid.plot_tiles(axes, fc='0.5', ec='none', alpha=alphas, zorder=1.1)

        # Add the tile outlines coloured by visibility
        ec = ['tab:blue' if tilename in event_tiles_vis
              else 'tab:red' if tilename in event_tiles
              else 'none'
              for tilename in grid_tiles]
        grid.plot_tiles(axes, fc='none', ec=ec, lw=1, zorder=1.21)

        # Add text
        if i == 0:
            axes.set_title(f'Tiling for survey {survey_name}', y=1.06)
            text = f'Showing site visibility for {(stop_time-start_time).to(u.h).value:.1f}h '
            text += f'starting {start_time.strftime("%Y-%m-%d %H:%M:%S")}'
            axes.text(0.5, 1.03, text, fontsize=8, ha='center', transform=axes.transAxes)
        axes.text(-0.03, -0.06, f'Site: {site_name}',
                  ha='left', va='bottom', transform=axes.transAxes)
        text = f'Visible tiles: {len(event_tiles_vis)}/{len(event_tiles)}\n'
        text += f'Visible probability: {visible_prob:.1%}'
        axes.text(0.78, -0.06, text, ha='left', va='bottom', transform=axes.transAxes)

    # Save
    direc = os.path.join(params.FILE_PATH, 'plots')
    if not os.path.exists(direc):
        os.makedirs(direc)
    filepath = os.path.join(direc, event.name + '_tiles.png')
    plt.savefig(filepath)
    plt.close(plt.gcf())

    # Send the message with the plot attached
    send_slack_msg(s, filepath=filepath, channel=slack_channel)
