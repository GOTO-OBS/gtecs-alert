"""Class for listening for transient alert notices."""

import itertools
import os
import socket
import sys
import threading
import time
import traceback
from urllib.error import URLError
from urllib.request import urlopen

import gcn.voeventclient as pygcn
import Pyro4
from gtecs.common import logging
from gtecs.common.daemon import BaseDaemon
from gtecs.common.pyro import get_uri, wait_for_uri
from gtecs.common.system import execute_long_command, get_pid, kill_process, make_pid_file
from gtecs.common.systemd import (
    SystemctlError,
    get_service,
    get_service_pid,
    kill_service,
    restart_service,
    start_service,
    stop_service,
)
from hop.auth import Auth, SASLMethod
from hop.io import StartPosition, Stream

from . import params
from .handler import already_in_database, handle_notice
from .notices import Notice
from .slack import send_slack_msg


@Pyro4.expose
class SentinelDaemon(BaseDaemon):
    """Sentinel alerts daemon class."""

    name = 'sentinel'

    def __init__(self):
        """Initialize the sentinel daemon."""
        super().__init__()

        # Sentinel properties
        self.latest_message_time = time.time()
        self.notice_queue = []
        self.latest_notice = None
        self.received_notices = 0
        self.processed_notices = 0
        self.ignored_roles = ['utility']
        if not params.PROCESS_TEST_NOTICES:  # TODO: could be an off/on switch?
            self.ignored_roles.append('test')

    def run(self, host, port, timeout=5):
        """Run the sentinel as a Pyro daemon."""
        # Start listener and handler threads
        listener_mode = 'KAFKA'  # TODO: params (or have both?)
        if listener_mode == 'SOCKET':
            listener_thread = threading.Thread(target=self._socket_listener_thread)
            listener_thread.daemon = True
            listener_thread.start()
        elif listener_mode == 'KAFKA':
            listener_thread = threading.Thread(
                target=self._kafka_listener_thread,
                args=(
                    params.KAFKA_USER,
                    params.KAFKA_PASSWORD,
                    params.KAFKA_BROKER,
                    params.KAFKA_GROUP_ID,
                    params.KAFKA_BACKDATE,
                ),
            )
            listener_thread.daemon = True
            listener_thread.start()
            # Also start a thread to monitor the latest message time
            heartbeat_thread = threading.Thread(target=self._kafka_heartbeat_thread)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
        else:
            raise ValueError('Listener mode must be "SOCKET" or "KAFKA"')
        handler_thread = threading.Thread(target=self._handler_thread)
        handler_thread.daemon = True
        handler_thread.start()

        # Start the daemon, will block until shutdown
        self.run_daemon(host, port, timeout=timeout)

        # If we reached here the loop has exited
        # Sleep for a second to make sure the threads have time to close
        time.sleep(1.0)

    # Internal threads
    def _socket_listener_thread(self):
        """Connect to a VOEvent Transport Protocol server and listen for VOEvents.

        Based on PyGCN's listen function:
        https://github.com/lpsinger/pygcn/blob/master/gcn/voeventclient.py

        """
        self.log.info('Alert listener thread started')

        # Define basic handler function to create a Notice instance and add it to the queue
        def _handler(payload, _root):
            self.latest_message_time = time.time()
            notice = Notice.from_payload(payload)
            self.notice_queue.append(notice)

        # Create a simple listen function, based on PyGCN's listen()
        # We have our own version here so we can have it in a thread with our own loop,
        # monitor if it's alive and close the socket when we shutdown
        def _listen(vo_socket, handler):
            try:
                while True:
                    pygcn._ingest_packet(vo_socket, params.LOCAL_IVO, handler, self.log)
            except socket.timeout:
                self.log.warning('Socket timed out')
            except socket.error:
                if self.running:
                    # It's only a problem if we're not the one shutting the socket
                    self.log.warning('Socket error')
            except Exception:
                self.log.exception('Error in alert listener')

        # This first while loop means the socket will be recreated if it closes.
        while self.running:
            # Create the socket, using the odd itertools loop PyGCN needs
            host_port = pygcn._validate_host_port(params.VOSERVER_HOST, params.VOSERVER_PORT)
            vo_socket = pygcn._open_socket(
                itertools.cycle(zip(*host_port)),
                log=self.log,
                iamalive_timeout=90,
                max_reconnect_timeout=8,
            )

            # Launch the listener within a new thread
            listener = threading.Thread(target=_listen, args=(vo_socket, _handler))
            listener.daemon = True
            listener.start()

            # This second loop will monitor the thread
            while self.running:
                if listener.is_alive():
                    time.sleep(1)
                else:
                    self.log.error('Alert listener failed')
                    break

            # Either the listener failed or self.running has been set to False
            # Close the socket nicely
            try:
                vo_socket.shutdown(socket.SHUT_RDWR)
            except socket.error:
                self.log.error('Could not shut down socket')
                self.log.debug('', exc_info=True)
            try:
                vo_socket.close()
            except socket.error:
                self.log.error('Could not close socket')
                self.log.debug('', exc_info=True)
            else:
                self.log.info('Closed socket connection')

        self.log.info('Alert listener thread stopped')

    def _kafka_listener_thread(
        self,
        user,
        password,
        broker='SCIMMA',
        group_id=None,
        backdate=False,
    ):
        """Connect to a Kafka server via SCiMMA HOPSKOTCH and listen for notices.

        This uses the Hop client (https://github.com/scimma/hop-client) which is built around
        Confluent Kafka (https://github.com/confluentinc/confluent-kafka-python).

        """
        self.log.info('Alert listener thread started')

        if broker == 'SCIMMA':
            broker_url = 'kafka://kafka.scimma.org/'
            # Subscribe to the combined GCN notice stream
            # Note this only receives "classic" text notices, not the new Unified schema ones (EP)
            # There's also no way to select specific notice types
            # SCIMMA have said they will be reworking their GCN system at some point...
            topics = ['gcn.notice']
            # Also subscribe to the heartbeat topic, so we can check if we're still connected
            topics += ['sys.heartbeat']
        elif broker == 'NASA':
            broker_url = 'kafka://kafka.gcn.nasa.gov/'
            # Subscribe only to the topics we're interested in
            topics = []
            # LVC/IGWN notices
            topics += [
                'igwn.gwalert',
            ]
            # Fermi notices
            # We only process the final positions for now
            topics += [
                'gcn.classic.voevent.FERMI_GBM_FLT_POS',
                'gcn.classic.voevent.FERMI_GBM_GND_POS',
                'gcn.classic.voevent.FERMI_GBM_FIN_POS',
            ]
            # Swift notices
            topics += [
                'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
            ]
            # GECAM notices
            # We only process the ground notices for now
            topics += [
                'gcn.classic.voevent.GECAM_FLT',
                'gcn.classic.voevent.GECAM_GND',
            ]
            # Einstein Probe notices
            # Note these use the new Unified schema, and are unavailable in the classic format
            topics += [
                'gcn.notices.einstein_probe.wxt.alert',
            ]
            # SVOM notices
            topics += [
                'gcn.notices.svom.voevent.grm',
                'gcn.notices.svom.voevent.eclairs',
            ]
            # IceCube notices
            topics += [
                'gcn.classic.voevent.ICECUBE_ASTROTRACK_BRONZE',
                'gcn.classic.voevent.ICECUBE_ASTROTRACK_GOLD',
                'gcn.classic.voevent.ICECUBE_CASCADE',
            ]
            # Also subscribe to the heartbeat topic, so we can check if we're still connected
            topics += ['gcn.heartbeat']
        else:
            raise ValueError('Broker must be "SCIMMA" or "NASA"')

        # This first while loop means the connection will be recreated if it fails.
        while self.running:
            # Create a Kafka stream
            if broker == 'SCIMMA':
                auth = Auth(user=user, password=password)
            elif broker == 'NASA':
                auth = Auth(
                    user=user,
                    password=password,
                    method=SASLMethod.OAUTHBEARER,
                    token_endpoint='https://auth.gcn.nasa.gov/oauth2/token',
                )
            group_id = auth.username + '-' + group_id
            if backdate:
                self.log.debug('Starting Kafka stream from earliest message')
                start_position = StartPosition.EARLIEST
                # # NB it would be great to backdate to a specific time as below.
                # # It was added in https://github.com/astronomy-commons/adc-streaming/pull/65
                # # but it doesn't seem to be implemented in hop-client yet.
                # start_position = datetime.now() - timedelta(hours=12)

                # One of the advantages of the newer brokers is monitoring the heartbeat topic
                # to make sure we're connected.
                # However, if we backdate with a new group ID we'll get weeks and weeks of
                # heartbeat messages, which is very annoying.
                # So instead we'll sneaky read the latest heartbeat message right now.
                # That gives the starting point for that topic, so when we start the stream
                # below it'll only have to handle a few seconds of heartbeat messages
                # rather than weeks.
                if broker == 'SCIMMA':
                    url = broker_url + 'sys.heartbeat'
                elif broker == 'NASA':
                    url = broker_url + 'gcn.heartbeat'
                stream = Stream(auth=auth, start_at=StartPosition.LATEST, until_eos=True)
                consumer = stream.open(url, mode='r', group_id=group_id)
                for payload, metadata in consumer.read_raw(metadata=True, autocommit=True):
                    if 'heartbeat' in metadata.topic:
                        break
            else:
                self.log.debug('Starting Kafka stream from latest message')
                start_position = StartPosition.LATEST
            stream = Stream(auth=auth, start_at=start_position, until_eos=False)

            # Now we connect to the stream and start reading messages
            try:
                url = broker_url + ','.join(topics)
                self.log.info(f'Connecting to Kafka stream at {url}')
                consumer = stream.open(url, mode='r', group_id=group_id)

                # Save the listed topics
                self.kafka_topics = topics
                self.kafka_all_topics = [
                    t for t in sorted(consumer._consumer._consumer.list_topics().topics.keys())
                ]

                for payload, metadata in consumer.read_raw(metadata=True, autocommit=True):
                    if not self.running:
                        break
                    # Because of the heartbeat messages we should be getting a message
                    # every few seconds, so we can use this timestamp to check if we're
                    # still connected.
                    self.latest_message_time = time.time()

                    if 'heartbeat' in metadata.topic:
                        # No need to process heartbeat messages
                        continue

                    # Create the notice and add it to the queue
                    try:
                        notice = Notice.from_payload(payload)
                        self.log.debug(f'Received notice: {notice.ivorn}')
                        self.notice_queue.append(notice)
                    except Exception as err:
                        self.log.error(f'Error creating notice: {err}')
                        self.log.debug(f'Payload: {payload}')
                        self.log.debug('', exc_info=True)
                        msg = 'Sentinel reports ERROR creating notice'
                        msg += f' ("{err.__class__.__name__}: {err}")'
                        send_slack_msg(msg)
                        # TODO: We could mark the message as unread if there's an error
                        # by using auto_commit=False.
                        # But the main processing is in the handler thread, so we won't know
                        # if that fails until it's too late.
                self.log.info('End of Kafka stream')

            except KeyboardInterrupt:
                self.log.info('Interrupt detected')
                pass
            except Exception as err:
                self.log.exception('Error in alert listener')
                self.log.debug('', exc_info=True)
                msg = 'Sentinel reports ERROR in alert listener'
                msg += f' ("{err.__class__.__name__}: {err}")'
                send_slack_msg(msg)
            finally:
                # Either the listener failed or self.running has been set to False
                # Make sure the connection is closed nicely
                try:
                    consumer.close()
                except UnboundLocalError:
                    # Consumer was never created, e.g. we couldn't connect to the broker
                    pass
                except Exception:
                    self.log.error('Could not close consumer')
                    self.log.debug('', exc_info=True)
                else:
                    self.log.info('Closed connection')

        self.log.info('Alert listener thread stopped')

    def _kafka_heartbeat_thread(self, timeout=60):
        """Monitor the latest message timestamp to check if we're still connected."""
        self.log.info('Heartbeat thread started')

        timed_out = False
        while self.running:
            # Because of the heartbeat messages we should be getting a message
            # every few seconds, so we can use this timestamp to check if we're
            # still connected.
            time_delta = time.time() - self.latest_message_time
            if time_delta > timeout:
                # We haven't received a message in a while.
                # We can't kill the listener thread from here, this is just for our own logging.
                self.log.warning(f'No new messages for {time_delta:.0f}s')
                if not timed_out:
                    # Only send the Slack message once
                    # Obviously this will fail if the network is down,
                    # it's more useful if there's an issue on the broker's end.
                    send_slack_msg(f'Sentinel reports no new messages for {time_delta:.0f}s')
                    timed_out = True
                    timed_out_time = time.time()
            else:
                if timed_out:
                    # We've started receiving messages again!
                    self.log.info('Connection restored')
                    time_delta = time.time() - timed_out_time
                    send_slack_msg(f'Sentinel connection restored after {time_delta:.0f}s')
                timed_out = False
            time.sleep(5)

        self.log.info('Heartbeat thread stopped')

    def _handler_thread(self):
        """Monitor the notice queue and handle any new notices."""
        self.log.info('Alert handler thread started')

        while self.running:
            if len(self.notice_queue) > 0:
                try:
                    # We have received a new notice
                    self.received_notices += 1
                    notice = self.notice_queue.pop(0)
                    self.latest_notice = notice
                    self.log.debug('Processing new notice: {}'.format(notice.ivorn))
                except Exception:
                    self.log.error('Error getting notice from queue')
                    self.log.debug('', exc_info=True)
                    continue

                try:
                    # Check if we want to process or ignore it
                    if notice.event_type == 'unknown':  # i.e. it's not one of the subclasses
                        self.log.debug('Ignoring unrecognised event class')
                        continue
                    elif notice.role in self.ignored_roles:
                        self.log.debug(f'Ignoring {notice.role} notice')
                        continue
                    elif already_in_database(notice):
                        self.log.debug('Ignoring already processed notice')
                        continue

                    send_slack_msg(f'Sentinel processing new notice ({notice.ivorn})')
                    handle_notice(
                        notice,
                        coincident_time_window=params.COINCIDENT_TIME_WINDOW,
                        send_messages=params.ENABLE_SLACK,
                        log=self.log,
                    )
                    self.processed_notices += 1
                    send_slack_msg('Sentinel successfully processed notice')

                    # Start a followup thread to wait for GWSkyNet scores
                    # Note we only check if the notice was already ignored
                    # (which also filters out RETRACTIONs) and has no GWSkyNet scores yet.
                    # It's also only valid for BAYESTAR skymaps (for now).
                    if (
                        notice.source == 'LVC'
                        and not notice.ivorn.endswith('_gwskynet')
                        and notice.strategy == 'IGNORE'
                        and notice.gwskynet is None
                        and notice.skymap is not None  # it should always be there by now?
                        and 'creator' in notice.skymap.header
                        and notice.skymap.header['creator'].lower() == 'bayestar'
                    ):
                        # The scores haven't been uploaded yet,
                        # but once they are it could be promoted
                        self.log.debug('Starting GWSkyNet listener thread')
                        t = threading.Thread(
                            target=self._gwskynet_thread,
                            args=[notice, 300],  # 5 mins should be enough
                        )
                        t.daemon = True
                        t.start()

                    # Start a followup thread to wait for the skymap of Fermi notices
                    if notice.source == 'Fermi' and not notice.ivorn.endswith('_new_skymap'):
                        try:
                            # Check if the URL was valid
                            urlopen(notice.skymap_url, timeout=5)
                        except (URLError, socket.timeout):
                            # The skymap hasn't been uploaded yet
                            self.log.debug('Starting Fermi skymap listener thread')
                            t = threading.Thread(
                                target=self._fermi_skymap_thread, args=[notice, 600]
                            )
                            t.daemon = True
                            t.start()

                except Exception as err:
                    self.log.error('Error handling notice')
                    self.log.debug(f'Payload: {notice.payload}')
                    self.log.debug('', exc_info=True)
                    msg = 'Sentinel reports ERROR handling notice'
                    msg += f' ("{err.__class__.__name__}: {err}")'
                    send_slack_msg(msg)

            time.sleep(0.1)

        self.log.info('Alert handler thread stopped')

    def _gwskynet_thread(self, notice, timeout=600):
        """Listen for the GWSkyNet scores for LVC notices."""
        self.log.info('{} GWSkyNet listener thread started'.format(notice.event_name))

        try:
            start_time = time.time()
            found_gwskynet = False
            timed_out = False
            while self.running and not found_gwskynet and not timed_out:
                # The logs are cached, so we need to delete them to redownload the list
                if hasattr(notice, '_gracedb_logs'):
                    delattr(notice, '_gracedb_logs')
                gwskynet = notice.get_gwskynet()
                if gwskynet is not None:
                    found_gwskynet = True
                    notice._gwskynet = gwskynet  # overwrite the cache
                    notice.ivorn = notice.ivorn + '_gwskynet'  # create a new ivorn for the DB
                else:
                    # no score file yet, sleep for 30s
                    time.sleep(30)
                if time.time() - start_time > timeout:
                    msg = '{} GWSkyNet listener thread timed out'.format(notice.event_name)
                    self.log.warning(msg)
                    # send_slack_msg(msg)  # This isn't worth a Slack message
                    timed_out = True

            if found_gwskynet:
                self.log.info('{} GWSkyNet listener thread successful'.format(notice.event_name))
                send_slack_msg('Re-ingesting LVC notice {}'.format(notice.event_name))
                self.notice_queue.append(notice)
                self.log.info('{} GWSkyNet listener thread finished'.format(notice.event_name))
            else:
                # Thread was shutdown before we found the score file, or timed out
                self.log.warning('{} GWSkyNet listener thread aborted'.format(notice.event_name))

        except Exception as err:
            self.log.exception('Error in {} GWSkyNet listener thread'.format(notice.event_name))
            msg = 'Sentinel reports ERROR in {} GWSkyNet listener thread'.format(notice.event_name)
            msg += f' ("{err.__class__.__name__}: {err}")'
            send_slack_msg(msg)

    def _fermi_skymap_thread(self, notice, timeout=600):
        """Listen for the official skymap for Fermi notices."""
        self.log.info('{} skymap listener thread started'.format(notice.event_name))

        try:
            start_time = time.time()
            found_skymap = False
            timed_out = False
            while self.running and not found_skymap and not timed_out:
                try:
                    urlopen(notice.skymap_url, timeout=5)
                    notice = Notice.from_payload(notice.payload)
                    notice.ivorn = notice.ivorn + '_new_skymap'  # create a new ivorn for the DB
                    found_skymap = True
                except (URLError, socket.timeout):
                    # if the link is not working yet, sleep for 30s
                    time.sleep(30)
                if time.time() - start_time > timeout:
                    msg = '{} skymap listener thread timed out'.format(notice.event_name)
                    self.log.warning(msg)
                    send_slack_msg(msg)
                    timed_out = True

            if found_skymap:
                send_slack_msg('Re-ingesting Fermi notice {}'.format(notice.event_name))
                self.notice_queue.append(notice)
                self.log.info('{} skymap listener thread finished'.format(notice.event_name))
            else:
                # Thread was shutdown before we found the skymap, or timed out
                self.log.warning('{} skymap listener thread aborted'.format(notice.event_name))

        except Exception as err:
            self.log.exception('Error in {} skymap listener thread'.format(notice.event_name))
            msg = 'Sentinel reports ERROR in {} skymap listener thread'.format(notice.event_name)
            msg += f' ("{err.__class__.__name__}: {err}")'
            send_slack_msg(msg)

    # Functions
    def ingest_from_payload(self, payload):
        """Ingest a notice payload."""
        notice = Notice.from_payload(payload)
        self.notice_queue.append(notice)
        return f'Notice {notice.ivorn} added to queue'

    def ingest_from_file(self, filepath):
        """Ingest a notice payload from a file."""
        notice = Notice.from_file(filepath)
        self.notice_queue.append(notice)
        return f'Notice {notice.ivorn} added to queue'

    def ingest_from_url(self, url):
        """Ingest a notice payload from a url."""
        notice = Notice.from_url(url)
        self.notice_queue.append(notice)
        return f'Notice {notice.ivorn} added to queue'

    def get_kafka_topics(self):
        """Return a list of subscribed topics."""
        return self.kafka_all_topics, self.kafka_topics

    def get_queue(self):
        """Return the current notice queue."""
        # Note: this is a list of IVORNs, not the full notice objects.
        # The Notice objects are not serializable.
        # We could return raw payloads I guess...
        return [notice.ivorn for notice in self.notice_queue]

    def clear_queue(self):
        """Clear the current notice queue."""
        queue_length = len(self.notice_queue)
        self.log.info(f'Clearing {queue_length} notices from queue')
        self.notice_queue = []
        return queue_length


def query(command, args):
    """Process a query."""
    # Primary control commands
    if command == 'run':
        service = get_service('sentinel')

        # Check the systemd service is already running
        if service is not None and service['active'] and service['pid'] != os.getpid():
            print(f'The sentinel service is already running (PID={service["pid"]})')
            print('Use "sentinel stop" to stop it before running the sentinel locally')
            return

        # Check if the Pyro URI is already registered
        uri = get_uri(params.PYRO_URI)
        if uri is not None:
            pid = get_service_pid('sentinel', 'sentinel')
            print(f'Sentinel is already running ({uri}, PID={pid})')
            return

        exit_code = 0
        with make_pid_file('sentinel'):
            try:
                send_slack_msg('Sentinel started')
                daemon = SentinelDaemon()
                daemon.run(params.PYRO_HOST, params.PYRO_PORT, params.PYRO_TIMEOUT)
            except Exception:
                print('Error detected, shutting down')
                traceback.print_exc()
                exit_code = 1
            except (KeyboardInterrupt, SystemExit):
                # SIGINT raises a KeyboardInterrupt, and make_pid_file makes SIGTERM raise
                # a SystemExit (which is how systemd stops the service),
                # so both are normal ways to stop.
                print('Interrupt detected, shutting down')
            finally:
                try:
                    daemon.shutdown()
                except UnboundLocalError:
                    # class was never created
                    pass
                time.sleep(1)  # wait to stop threads
                send_slack_msg('Sentinel shutdown')
                print('Sentinel done')

            sys.exit(exit_code)
        return

    if command == 'start':
        service = get_service('sentinel')

        # Check if the systemd service is set up
        if service is None:
            print('ERROR: Cannot connect to the systemd service, or it is not installed')
            print('Use "sentinel run" to run the sentinel in this terminal instead')
            return

        # Check if the Pyro URI is already registered
        uri = get_uri(params.PYRO_URI)
        if uri is not None:
            pid = get_service_pid('sentinel', 'sentinel')
            print(f'Sentinel is already running ({uri}, PID={pid})')
            return

        start_service('sentinel', sudo=params.SYSTEMD_SUDO)

        # Check if the service is now running
        uri = wait_for_uri(params.PYRO_URI)
        if uri is None:
            print('ERROR: The sentinel service started, but it is not responding')
            print('Check the service with "systemctl status sentinel"')
        else:
            pid = get_service_pid('sentinel', 'sentinel')
            print(f'Sentinel started ({uri}, PID={pid})')
        return

    if command in ['shutdown', 'stop']:
        service = get_service('sentinel')

        # Check if the systemd service is set up
        if service is None:
            print('ERROR: Cannot connect to the systemd service, or it is not installed')
            return

        if service['active']:
            # The service is running through systemd, so shut it down through systemd
            stop_service('sentinel', sudo=params.SYSTEMD_SUDO)
            print('Sentinel shutdown')
            return

        # It might might be running independently in a terminal,
        # so try shutting down though Pyro
        try:
            with Pyro4.Proxy(params.PYRO_URI) as proxy:
                proxy.shutdown()
                print('Sentinel shutdown')
        except Pyro4.errors.CommunicationError:
            print('Sentinel is not running')
        return

    if command == 'restart':
        service = get_service('sentinel')

        # Check if the systemd service is set up
        if service is None:
            print('ERROR: Cannot connect to the systemd service, or it is not installed')
            return

        restart_service('sentinel', sudo=params.SYSTEMD_SUDO)

        # Check if the service is now running
        uri = wait_for_uri(params.PYRO_URI)
        if uri is None:
            print('ERROR: The sentinel service restarted, but it is not responding')
            print('Check the service with "systemctl status sentinel"')
        else:
            pid = get_service_pid('sentinel', 'sentinel')
            print(f'Sentinel restarted ({uri}, PID={pid})')
        return

    if command == 'kill':
        service = get_service('sentinel')
        if service is not None and service['active']:
            # The service is running through systemd, so we need to kill it through systemd
            kill_service('sentinel', sudo=params.SYSTEMD_SUDO)
            print('Sentinel killed (note systemd may restart it)')
            return

        if get_pid('sentinel'):
            # It might might be running independently in a terminal,
            # so try killing it directly
            kill_process('sentinel')
            return

        print('Sentinel is not running')
        return

    if command in ['ping', 'status']:
        uri = get_uri(params.PYRO_URI)
        if uri is not None:
            pid = get_service_pid('sentinel', 'sentinel')
            print(f'Sentinel is running ({uri}, PID={pid})')
            with Pyro4.Proxy(params.PYRO_URI) as proxy:
                start_time = proxy.start_time
            print(f'Uptime: {time.time() - start_time:.1f}s')
        else:
            print('Sentinel is not running')

        service = get_service('sentinel')
        if service is not None:
            print('Service state: {} ({})'.format(service['active_state'], service['sub_state']))
        return

    if command in ['log', 'tail']:
        log_file = 'sentinel.log'
        log_path = logging.get_log_path() / log_file
        tail_command = 'tail {} {}'.format(log_path, ' '.join(args))

        execute_long_command(tail_command)
        return

    # Sentinel commands
    if command == 'ingest':
        if len(args) != 1:
            raise ValueError('Invalid arguments: sentinel ingest [path]')
        with Pyro4.Proxy(params.PYRO_URI) as proxy:
            try:
                print(proxy.ingest_from_file(args[0]))
            except FileNotFoundError:
                try:
                    print(proxy.ingest_from_url(args[0]))
                except Exception:
                    print(f'Cound not find file or URL for {args[0]}')
                    raise
        return

    if command == 'queue':
        if len(args) == 0:
            with Pyro4.Proxy(params.PYRO_URI) as proxy:
                queue = proxy.get_queue()
            print(f'There are {len(queue)} notices currently in the queue')
            for i, ivorn in enumerate(queue):
                print(i, ivorn)
        elif len(args) == 1 and args[0] == 'clear':
            with Pyro4.Proxy(params.PYRO_URI) as proxy:
                queue_length = proxy.clear_queue()
                print(f'Cleared {queue_length} notices from the queue')
        else:
            print('ERROR: Invalid arguments for "queue" command')
            print('Usage: sentinel queue [clear]')
        return

    if command == 'topics':
        with Pyro4.Proxy(params.PYRO_URI) as proxy:
            available, requested = proxy.get_kafka_topics()
        for topic in available:
            print(topic + (' (subscribed)' if topic in requested else ''))
        return

    # Help or unknown command - print instructions
    if command in ['help', '?']:
        print('The sentinel listens for alerts and inserts them into the database.\n')
    else:
        print(f'ERROR: Unrecognized command "{command}"\n')
    print('Usage: sentinel [command] [options]\n')
    print('Sentinel commands:')
    print('   ingest [path|ivorn]    add the given notice to the queue')
    print('   queue                  print the notices currently in the queue')
    print('   queue clear            clear all notices currently in the queue')
    print('   topics                 print all subscribed Kafka topics')
    print('Control commands:')
    print('   start                  start the sentinel systemd service')
    print('   stop/shutdown          stop the sentinel systemd service')
    print('   restart                restart the sentinel systemd service')
    print('   kill                   kill the sentinel systemd service (systemd may restart it)')
    print('   run                    run the sentinel daemon locally in this terminal')
    print('   ping/status            check if the sentinel daemon or service is running')
    print('   log [tail args]        print log file contents (alias for tail)')
    print('   help                   print these instructions')


def main():
    """Main entry point for the script."""
    if len(sys.argv) > 1:
        command, *args = sys.argv[1:]
    else:
        # Interpret no arguments as a request for help
        command, args = 'help', []

    try:
        query(command, args)
    except (SystemctlError, ConnectionError) as err:
        print(f'ERROR: {err}')
        sys.exit(1)
