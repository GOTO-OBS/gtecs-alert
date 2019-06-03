"""Slack messaging tools."""

import os

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
    """TODO."""
    pass
