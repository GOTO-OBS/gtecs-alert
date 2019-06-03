"""Slack messaging tools."""

import os

from astroplan import AltitudeConstraint, AtNightConstraint, Observer, is_observable

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

import numpy as np

import obsdb as db

from slackclient import SlackClient

from . import params

BOT_TOKEN = params.SLACK_BOT_TOKEN
BOT_NAME = params.SLACK_BOT_NAME
CHANNEL_NAME = params.SLACK_BOT_CHANNEL


def send_slack_msg(text, attachments=None, filepath=None):
    """Send a message to Slack, using the settings defined in `gotoalert.params`.

    Parameters
    ----------
    text : string
        The message text.

    attachments : dict, optional
        Attachments to the message.
        NB a message can have attachments OR a file, not both.

    filepath : string, optional
        A local path to a file to be added to the message.
        NB a message can have a file OR attachments, not both.

    """
    if attachments is not None and filepath is not None:
        raise ValueError("A Slack message can't have both attachments and a file.")

    if params.ENABLE_SLACK:
        client = SlackClient(BOT_TOKEN)
        try:
            if not filepath:
                api_call = client.api_call('chat.postMessage',
                                           channel=CHANNEL_NAME,
                                           username=BOT_NAME,
                                           as_user=True,
                                           text=text,
                                           attachments=attachments,
                                           )
            else:
                filename = os.path.basename(filepath)
                name = os.path.splitext(filename)[0]
                with open(filepath, 'rb') as file:
                    api_call = client.api_call('files.upload',
                                               channels=CHANNEL_NAME,  # Note channel(s)
                                               username=BOT_NAME,
                                               as_user=True,
                                               initial_comment=text,
                                               filename=filename,
                                               file=file,
                                               title=name,
                                               )
            if not api_call.get('ok'):
                raise Exception('Unable to send message')
        except Exception as err:
            print('Connection to Slack failed! - {}'.format(err))
            print('Message:', text)
            print('Attachments:', attachments)
            print('Filepath:', filepath)
    else:
        print('Slack Message:', text)
        print('Attachments:', attachments)
        print('Filepath:', filepath)


def send_event_report(event):
    """Send a message to Slack with the event details and skymap."""
    title = ['*Details for event {}*'.format(event.name)]

    # Basic details
    details = ['IVORN: {}'.format(event.ivorn),
               'Event time: {}'.format(event.time.iso),
               ]

    # Extra details, depending on source type
    extra_details = []
    if event.type == 'GW' and event.group == 'CBC':
        sorted_class = sorted(event.classification.keys(),
                              key=lambda key: event.classification[key],
                              reverse=True)
        class_str = ', '.join(['{}:{:.1f}%'.format(key, event.classification[key] * 100)
                              for key in sorted_class
                              if event.classification[key] > 0.0005])
        extra_details = ['Group: CBC',
                         'FAR: ~1 per {:.1f} yrs'.format(1 / event.far / 3.154e+7),
                         'Distance: {:.0f}+/-{:.0f} Mpc'.format(event.distance,
                                                                event.distance_error),
                         'Classification: {}'.format(class_str),
                         'HasNS (if real): {:.0f}%'.format(event.properties['HasNS'] * 100),
                         '90% probability area: {:.0f} sq deg'.format(event.contour_areas[0.9]),
                         'GraceDB page: {}'.format(event.gracedb_url),
                         ]
    elif event.type == 'GW':
        # Burst alerts, much less info
        extra_details = ['Group: Burst',
                         'FAR: ~1 per {:.1f} yrs'.format(1 / event.far / 3.154e+7),
                         '90% probability area: {:.0f} sq deg'.format(event.contour_areas[0.9]),
                         'GraceDB page: {}'.format(event.gracedb_url),
                         ]
    elif event.type == 'GW_RETRACTION':
        # Note clearly it's a retraction event
        extra_details = ['GraceDB page: {}'.format(event.gracedb_url),
                         '*THIS IS A RETRACTION OF EVENT {}*'.format(event.id),
                         ]
    elif event.type == 'GRB':
        # GRB events should have a given location
        extra_details = ['Position: {} ({})'.format(event.coord.to_string('hmsdms'),
                                                    event.coord.to_string()),
                         'Position error: {:.3f}'.format(event.total_error),
                         ]
        if event.source == 'Fermi':
            # Fermi events should have a duration parameter
            extra_details += ['Duration: {}'.format(event.duration.capitalize()),
                              ]

    details += extra_details

    message_text = '\n'.join(title + details)

    # Plot skymap
    filepath = None
    if event.skymap is not None:
        # Some events (retractions) won't have skymaps
        filename = event.name + '_skymap.png'
        filepath = os.path.join(params.FILE_PATH, filename)
        event.skymap.plot(filename=filepath)

    # Send the message, with the skymap file attached
    send_slack_msg(message_text, filepath=filepath)


def send_strategy_report(event):
    """Send a message to Slack with the event strategy details."""
    title = ['*Strategy for event {}*'.format(event.name)]

    # Basic details
    strategy = event.strategy
    details = ['Strategy: `{}`'.format(strategy['strategy']),
               'Rank: {}'.format(strategy['rank']),
               'Cadence: `{}`'.format(strategy['cadence']),
               '- Number of visits: {}'.format(strategy['cadence_dict']['num_todo']),
               '- Time between visits (mins): {}'.format(strategy['cadence_dict']['wait_time']),
               '- Start time: {}'.format(strategy['start_time'].iso),
               '- Stop time: {}'.format(strategy['stop_time'].iso),
               'Constraints: `{}`'.format(strategy['constraints']),
               '- Min Alt: {}'.format(strategy['constraints_dict']['min_alt']),
               '- Max Sun Alt: {}'.format(strategy['constraints_dict']['max_sunalt']),
               '- Min Moon Sep: {}'.format(strategy['constraints_dict']['min_moonsep']),
               '- Max Moon Phase: {}'.format(strategy['constraints_dict']['max_moon']),
               'ExposureSets: `{}`'.format(strategy['exposure_sets']),
               ]
    for expset in strategy['exposure_sets_dict']:
        details.append('- NumExp: {:.0f}  Filter: {}  ExpTime: {:.1f}s'.format(expset['num_exp'],
                                                                               expset['filt'],
                                                                               expset['exptime'],
                                                                               ))

    message_text = '\n'.join(title + details)

    # Send the message, with the skymap file attached
    send_slack_msg(message_text)


def send_database_report(event):
    """Send a message to Slack with details of the database pointings and visibility."""
    title = ['*Visibility for event {}*'.format(event.name)]

    # Basic details
    details = ['Inserted on Grid: {}'.format(event.strategy['on_grid']),
               'Expired: {}'.format(event.strategy['stop_time'] < Time.now())]

    filepath = None
    with db.open_session() as session:
        # Query Event table entries
        db_events = session.query(db.Event).filter(db.Event.name == event.name).all()

        details += ['Number of Events in database: {}'.format(len(db_events))]

        if len(db_events) == 0:
            # Uh-oh
            details += ['*ERROR: Nothing found in database*']
        else:
            # This event should be the latest added
            db_event = db_events[-1]

            # Get Mpointings
            db_mpointings = db_event.mpointings

            details += ['Number of targets for this event: {}'.format(len(db_mpointings))]

            if len(db_mpointings) == 0:
                # Uh-oh
                details += ['*ERROR: No Mpointings found in database*']
            else:
                # Get the Mpointing coordinates
                ras = [mpointing.ra for mpointing in db_mpointings]
                decs = [mpointing.dec for mpointing in db_mpointings]
                coords = SkyCoord(ras, decs, unit='deg')

                for site in ['La Palma']:  # TODO: should be in params
                    details += ['Predicted visibility from {}:'.format(site)]

                    # Create Astroplan Observer
                    observer = Observer.at_site(site.lower().replace(' ', ''))

                    # Create visibility constraints
                    min_alt = float(event.strategy['constraints_dict']['min_alt']) * u.deg
                    max_sunalt = float(event.strategy['constraints_dict']['max_sunalt']) * u.deg
                    alt_constraint = AltitudeConstraint(min=min_alt)
                    night_constraint = AtNightConstraint(max_solar_altitude=max_sunalt)
                    constraints = [alt_constraint, night_constraint]

                    # Check visibility until the stop time
                    start_time = event.strategy['start_time']
                    stop_time = event.strategy['stop_time']
                    mps_visible_mask = is_observable(constraints, observer, coords,
                                                     time_range=[start_time, stop_time])

                    details += ['- Targets visible during valid period: {}/{}'.format(
                        sum(mps_visible_mask), len(db_mpointings))]

                    if event.strategy['on_grid']:
                        # Find the total probibility for all tiles
                        mp_tiles = np.array([mp.grid_tile.name for mp in db_mpointings])
                        total_prob = event.grid.get_probability(list(mp_tiles)) * 100
                        details += ['- Total probability in all tiles: {:.1f}%'.format(total_prob)]

                        # Get visible mp tile names
                        mp_tiles_visible = mp_tiles[mps_visible_mask]
                        visible_prob = event.grid.get_probability(list(mp_tiles_visible)) * 100
                        details += ['- Probability in visible tiles: {:.1f}%'.format(visible_prob)]

                        # Get non-visible mp tile names
                        mps_notvisible_tonight_mask = np.invert(mps_visible_mask)
                        mp_tiles_notvisible = mp_tiles[mps_notvisible_tonight_mask]

                        # Get all non-visible tiles
                        tiles_visible_mask = is_observable(constraints, observer, event.grid.coords,
                                                           time_range=[start_time, stop_time])
                        tiles_notvisible_mask = np.invert(tiles_visible_mask)
                        tiles_notvisible = np.array(event.grid.tilenames)[tiles_notvisible_mask]

                        # Create a plot of the tiles, showing visibility tonight
                        # TODO: multiple sites? Need multiple plots or one combined?
                        filename = event.name + '_tiles.png'
                        filepath = os.path.join(params.FILE_PATH, filename)
                        event.grid.plot(filename=filepath,
                                        plot_skymap=True,
                                        highlight=[mp_tiles_visible, mp_tiles_notvisible],
                                        highlight_color=['blue', 'red'],
                                        color={tilename: '0.5' for tilename in tiles_notvisible},
                                        )

    message_text = '\n'.join(title + details)

    # Send the message, with the plot attached if one was generated
    send_slack_msg(message_text, filepath=filepath)
