"""Class for listening for GCN alert notices."""

import itertools
import socket
import threading
import time
import traceback
from urllib.request import URLError, urlopen

import Pyro4

import gcn.voeventclient as pygcn

from gcn_kafka import Consumer

from gtecs.common import logging

from . import params
from .gcn import GCNNotice
from .handler import handle_notice
from .slack import send_slack_msg


@Pyro4.expose
class Sentinel:
    """Sentinel alerts daemon class."""

    def __init__(self):
        # get a logger for the sentinel
        self.log = logging.get_logger('sentinel')
        self.log.info('Sentinel started')

        # sentinel variables
        self.running = False
        self.notice_queue = []
        self.latest_notice = None
        self.received_notices = 0
        self.processed_notices = 0
        self.ignored_roles = ['utility']
        if not params.PROCESS_TEST_NOTICES:  # TODO: could be an off/on switch?
            self.ignored_roles.append('test')

    def __del__(self):
        self.shutdown()

    def run(self, host, port, timeout=5):
        """Run the sentinel as a Pyro daemon."""
        self.running = True

        # Start threads
        if params.KAFKA_CLIENT_ID != 'unknown':  # TODO: Switch, or even have multiple?
            t1 = threading.Thread(target=self._kafka_listener_thread)
        else:
            t1 = threading.Thread(target=self._socket_listener_thread)
        t1.daemon = True
        t1.start()

        t2 = threading.Thread(target=self._handler_thread)
        t2.daemon = True
        t2.start()

        # Check the Pyro address is available
        try:
            pyro_daemon = Pyro4.Daemon(host, port)
        except Exception:
            raise
        else:
            pyro_daemon.close()

        # Start the daemon
        with Pyro4.Daemon(host, port) as pyro_daemon:
            self._uri = pyro_daemon.register(self, objectId='sentinel')
            Pyro4.config.COMMTIMEOUT = timeout

            # Start request loop
            self.log.info('Pyro daemon registered to {}'.format(self._uri))
            pyro_daemon.requestLoop(loopCondition=self.is_running)

        # Loop has closed
        self.log.info('Pyro daemon successfully shut down')
        time.sleep(1.)

    def is_running(self):
        """Check if the daemon is running or not.

        Used for the Pyro loop condition, it needs a function so you can't just
        give it self.running.
        """
        return self.running

    @property
    def uri(self):
        """Return the Pyro URI."""
        if hasattr(self, '_uri'):
            return self._uri
        else:
            return None

    def shutdown(self):
        """Shut down the running threads."""
        self.running = False

    # Internal threads
    def _socket_listener_thread(self):
        """Connect to a VOEvent Transport Protocol server and listen for VOEvents.

        Based on PyGCN's listen function:
        https://github.com/lpsinger/pygcn/blob/master/gcn/voeventclient.py

        """
        self.log.info('Alert listener thread started')

        # Define basic handler function to create a GCNNotice instance and add it to the queue
        def _handler(payload, root):
            notice = GCNNotice.from_payload(payload)
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
            vo_socket = pygcn._open_socket(itertools.cycle(zip(*host_port)),
                                           log=self.log,
                                           iamalive_timeout=90,
                                           max_reconnect_timeout=8)

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
        return

    def _kafka_listener_thread(self):
        """Connect to a Kafka server and listen for notices.

        This uses GCN Kafka (https://github.com/nasa-gcn/gcn-kafka-python) which is built around
        Confluent Kafka (https://github.com/confluentinc/confluent-kafka-python).

        """
        self.log.info('Alert listener thread started')

        # This first while loop means the connection will be recreated if it fails.
        while self.running:
            # Create a Kafka Consumer
            consumer = Consumer(client_id=params.KAFKA_CLIENT_ID,
                                client_secret=params.KAFKA_CLIENT_SECRET
                                )

            # Subscribe to any notices we want
            # TODO: Also params? Or we could get from the subclasses?
            #       For now just subscribe to everything...
            all_topics = [t for t in consumer.list_topics().topics.keys() if 'voevent' in t]
            consumer.subscribe(all_topics)

            # This second loop will monitor the connection
            try:
                while self.running:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        # self.log.info('Waiting...')
                        continue
                    if msg.error():
                        self.log.error(msg.error())
                    else:
                        # Add to the queue
                        payload = msg.value()
                        notice = GCNNotice.from_payload(payload)
                        self.notice_queue.append(notice)
            except KeyboardInterrupt:
                pass
            except Exception:
                self.log.exception('Error in alert listener')
            finally:
                # Either the listener failed or self.running has been set to False
                # Make sure the connection is closed nicely
                try:
                    consumer.close()
                except Exception:
                    self.log.error('Could not close consumer')
                    self.log.debug('', exc_info=True)
                else:
                    self.log.info('Closed connection')

        self.log.info('Alert listener thread stopped')
        return

    def _handler_thread(self):
        """Monitor the notice queue and handle any new notices."""
        self.log.info('Alert handler thread started')

        while self.running:
            # Check the notice queue, take off the first entry
            if len(self.notice_queue) > 0:
                # There's at least one new notice!
                self.received_notices += 1
                notice = self.notice_queue.pop(0)
                self.latest_notice = notice

                self.log.debug('Processing new notice: {}'.format(notice.ivorn))

                # Check if it's one we want to handle
                if notice.event_type == 'unknown':  # i.e. it's not one of the subclasses
                    self.log.debug('Ignoring unrecognised event class')
                    continue
                elif notice.role in self.ignored_roles:
                    self.log.debug(f'Ignoring {notice.role} notice')
                    continue

                try:
                    send_slack_msg(f'Sentinel: Processing new notice {notice.ivorn}')
                    # Call the handler
                    handle_notice(notice, send_messages=params.ENABLE_SLACK, log=self.log)

                    # If we got here it worked
                    self.processed_notices += 1

                    # Start a followup thread to wait for the skymap of Fermi notices
                    if notice.event_source == 'Fermi':
                        try:
                            # Might as well try once
                            urlopen(notice.skymap_url)
                        except URLError:
                            # The skymap hasn't been uploaded yet
                            try:
                                t = threading.Thread(target=self._fermi_skymap_thread(notice))
                                t.daemon = True
                                t.start()
                            except Exception:
                                self.log.exception('Error in Fermi followup thread')

                    send_slack_msg(f'Sentinel: Successfully processed notice {notice.ivorn}')
                except Exception:
                    self.log.exception('Error handling notice {}'.format(notice.ivorn))
                    send_slack_msg(f'Sentinel: ERROR handling notice {notice.ivorn}')

            time.sleep(0.1)

        self.log.info('Alert handler thread stopped')
        return

    def _fermi_skymap_thread(self, notice):
        """Listen for the official skymap for Fermi notices."""
        self.log.info('{} skymap listening thread started'.format(notice.event_name))

        found_skymap = False
        while self.running and not found_skymap:
            try:
                urlopen(notice.skymap_url)
                notice = GCNNotice.from_payload(notice.payload)
                notice.ivorn = notice.ivorn + '_new_skymap'  # create a new ivorn for the DB
                found_skymap = True
            except URLError:
                # if the link is not working yet, sleep for 30s
                time.sleep(30)

        if found_skymap:
            try:
                # Call the handler for the new notice
                # TODO: could just add to the queue?
                handle_notice(notice, send_messages=params.ENABLE_SLACK, log=self.log)
                send_slack_msg('Latest skymap used for {}'.format(notice.event_name))
            except Exception:
                self.log.exception('Exception in handler')
            self.log.info('{} skymap listening thread finished'.format(notice.event_name))
        else:
            # Thread was shutdown before we found the skymap
            self.log.info('{} skymap listening thread aborted'.format(notice.event_name))
            return

    # Functions
    def ingest_from_payload(self, payload):
        """Ingest a VOEvent payload."""
        notice = GCNNotice.from_payload(payload)
        self.notice_queue.append(notice)
        return 'VOEvent notice added to queue'

    def ingest_from_file(self, filepath):
        """Ingest a VOEvent payload from a file."""
        notice = GCNNotice.from_file(filepath)
        self.notice_queue.append(notice)
        return 'VOEvent notice added to queue'

    def ingest_from_ivorn(self, ivorn):
        """Ingest a VOEvent payload from its IVORN.

        Will attempt to download the payload from the 4pisky VOEvent DB.
        """
        notice = GCNNotice.from_ivorn(ivorn)
        self.notice_queue.append(notice)
        return 'VOEvent notice added to queue'


def run():
    """Start the sentinel."""
    try:
        send_slack_msg('Sentinel started')
        sentinel = Sentinel()
        sentinel.run(params.PYRO_HOST, params.PYRO_PORT, params.PYRO_TIMEOUT)
    except Exception:
        print('Error detected, shutting down')
        traceback.print_exc()
    except KeyboardInterrupt:
        print('Interrupt detected, shutting down')
    finally:
        try:
            sentinel.shutdown()
        except UnboundLocalError:
            # class was never created
            pass
        time.sleep(1)  # wait to stop threads
        send_slack_msg('Sentinel shutdown')
        print('Sentinel done')
