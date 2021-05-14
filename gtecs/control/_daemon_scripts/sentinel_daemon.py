#!/usr/bin/env python3
"""Daemon to listen for alerts and insert them into the database."""

import os
import socket
import threading
import time

from astropy.time import Time

import gcn.voeventclient as pygcn
import voeventparse as vp

from gtecs.alert.events import Event
from gtecs.alert.handler import event_handler
from gtecs.control import misc
from gtecs.control import params
from gtecs.control.daemons import BaseDaemon
from gtecs.control.slack import send_slack_msg

from urllib.request import urlopen

class SentinelDaemon(BaseDaemon):
    """Sentinel alerts daemon class."""

    def __init__(self):
        super().__init__('sentinel')

        # sentinel variables
        self.listening = True
        self.events_queue = []
        self.latest_event = None
        self.processed_events = 0
        self.interesting_events = 0

        # start control thread
        t = threading.Thread(target=self._control_thread)
        t.daemon = True
        t.start()

        # start alert listener thread
        t2 = threading.Thread(target=self._alert_listener_thread)
        t2.daemon = True
        t2.start()

    # Primary control thread
    def _control_thread(self):
        """Primary control loop."""
        self.log.info('Daemon control thread started')

        while(self.running):
            self.loop_time = time.time()

            # system check
            if self.force_check_flag or (self.loop_time - self.check_time) > self.check_period:
                self.check_time = self.loop_time
                self.force_check_flag = False

                # Nothing to connect to, just get the info
                self._get_info()

            # sentinel processes
            # check the events queue, take off the first entry
            if len(self.events_queue) > 0:
                # There's at least one new event!
                self.latest_event = self.events_queue.pop(0)
                self.log.info('Processing new event: {}'.format(self.latest_event.ivorn))
                event_handled = False
                try:
                    self._handle_event()
                    event_handled = True

                except Exception:
                    self.log.error('handle_event command failed')
                    self.log.debug('', exc_info=True)

                # new thread that listens the skymap of Fermin events
                # no point to start the skymap listening thread if the event hasn't been handled
                if self.latest_event.source == 'Fermi' and event_handled:
                    try:
                        self.Fermi_skymap_listening = True
                        Fermi_skymap_listener = threading.Thread(target=self._Fermi_skymap_thread(event=self.latest_event))
                        Fermi_skymap_listener.daemon = True
                        Fermi_skymap_listener.start()
                    except Exception:
                        self.log.error('error in Fermi skymap listener')

            time.sleep(params.DAEMON_SLEEP_TIME)  # To save 100% CPU usage

        self.log.info('Daemon control thread stopped')
        return

    # Secondary threads
    def _alert_listener_thread(self):
        """Connect to a VOEvent Transport Protocol server and listen for VOEvents.

        Based on PyGCN's listen function:
        https://github.com/lpsinger/pygcn/blob/master/gcn/voeventclient.py

        """
        self.log.info('Alert listener thread started')

        # Define a handler function
        # All we need this to do is create and Event and add it to the queue
        def _handler(payload, root):
            event = Event.from_payload(payload)
            self.events_queue.append(event)

        # This first while loop means the socket will be recreated if it closes.
        while self.running:
            # Only listen if self.listening is True
            if self.listening:
                # Create the socket
                vo_socket = pygcn._open_socket(params.VOSERVER_HOST, params.VOSERVER_PORT,
                                               log=self.log,
                                               iamalive_timeout=90,
                                               max_reconnect_timeout=8)

                # Create a simple listen function
                def _listen(vo_socket, handler):
                    try:
                        while True:
                            pygcn._ingest_packet(vo_socket, params.LOCAL_IVO, handler, self.log)
                    except socket.timeout:
                        self.log.warning('socket timed out')
                    except socket.error:
                        if self.running and self.listening:
                            # It's only a problem if we're not the one shutting the socket
                            self.log.warning('socket error')
                    except Exception as err:
                        self.log.error('Error in alert listener')
                        self.log.debug('', exc_info=True)

                # launch the listener within a new thread
                listener = threading.Thread(target=_listen, args=(vo_socket, _handler))
                listener.daemon = True
                listener.start()

                # This second loop will monitor the thread
                while self.running and self.listening:
                    if listener.is_alive():
                        time.sleep(1)
                    else:
                        self.log.error('Alert listener failed')
                        break

                # Either the listener failed or listening or running have been set to False
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
                    self.log.info('closed socket connection')

            else:
                self.log.warning('Alert listener paused')
                time.sleep(2)

        self.log.info('Alert listener thread stopped')
        return

    # A thread to listen the official skymap for Fermi events
    def _Fermi_skymap_thread(self, event):
        top_params = vp.get_toplevel_params(event.voevent)
        lightcurve_url = top_params['LightCurve_URL']['value']
        skymap_url = lightcurve_url.replace('lc_medres34', 'healpix_all').replace('.gif', '.fit')
        self.log.info('{}_{} skymap listening thread started'.format(event.source, event.id))

        while self.running and self.Fermi_skymap_listening:
            try: 
                urlopen(skymap_url)
                #send_slack_msg('latest skymap used for {}_{}'.format(event.soucre, event.id))
                event.ivorn = event.ivorn + '_new_skymap' # assign a new ivorn
                event_handler(event, log=self.log) # This 'new' event will not be sent on slack
                self.log.info('latest skymap used for {}_{}'.format(event.source, event.id))
                self.Fermi_skymap_listening = False
                self.log.info('{}_{} skymap listening thread finished'.format(event.source, event.id))
            except:
                # if the link is not working yet, sleep for 30s
                time.sleep(30)
        if self.Fermi_skymap_listening and not self.running:
            self.log.info('Fermi skymap listener thread stopped') 
            return

    # Internal functions
    def _get_info(self):
        """Get the latest status info from the heardware."""
        temp_info = {}

        # Get basic daemon info
        temp_info['daemon_id'] = self.daemon_id
        temp_info['time'] = self.loop_time
        temp_info['timestamp'] = Time(self.loop_time, format='unix', precision=0).iso
        temp_info['uptime'] = self.loop_time - self.start_time

        # Get internal info
        if self.listening:
            temp_info['status'] = 'Listening'
        else:
            temp_info['status'] = 'Paused'
        temp_info['pending_events'] = len(self.events_queue)
        temp_info['latest_event'] = self.latest_event
        temp_info['processed_events'] = self.processed_events
        temp_info['interesting_events'] = self.interesting_events

        # Write debug log line
        try:
            now_str = '{} ({} processed, {} interesting)'.format(temp_info['status'],
                                                                 temp_info['processed_events'],
                                                                 temp_info['interesting_events'])
            if not self.info:
                self.log.debug('Sentinel is {}'.format(now_str))
            else:
                old_str = '{} ({} processed, {} interesting)'.format(
                    self.info['status'],
                    self.info['processed_events'],
                    self.info['interesting_events'])
                if now_str != old_str:
                    self.log.debug('Sentinel is {}'.format(now_str))
        except Exception:
            self.log.error('Could not write current status')

        # Update the master info dict
        self.info = temp_info

        # Finally check if we need to report an error
        self._check_errors()

    def _handle_event(self):
        """Archive each VOEvent, then pass it to GOTO-alert."""
        event = self.latest_event

        # Archive the event
        path = os.path.join(params.FILE_PATH, 'voevents')
        event.archive(path)
        self.log.info('Archived to {}'.format(path))

        # If the event's not interesting we don't care
        if event.interesting:
            # Call GOTO-alert's event handler
            # TODO: Alert messages should go to a different channel
            try:
                send_slack_msg('Sentinel is processing event {}'.format(event.ivorn),
                               channel=params.SENTINEL_SLACK_CHANNEL)
                event_handler(event, send_messages=params.SENTINEL_SEND_MESSAGES, log=self.log)
            except Exception as err:
                self.log.error('Exception in event handler')
                self.log.exception(err)
                send_slack_msg('Sentinel reports exception in event handler, check logs',
                               channel=params.SENTINEL_SLACK_CHANNEL)
                return

            self.log.info('Interesting event {} processed'.format(event.name))
            self.interesting_events += 1

        # Done!
        self.processed_events += 1

    # Control functions
    def ingest_from_payload(self, payload):
        """Ingest an event payload."""
        event = Event.from_payload(payload)
        self.events_queue.append(event)
        return 'Event added to queue'

    def ingest_from_ivorn(self, ivorn):
        """Ingest an event from its IVORN.

        Will attempt to download the event payload from the 4pisky VOEvent DB.
        """
        event = Event.from_ivorn(ivorn)
        self.events_queue.append(event)
        return 'Event added to queue'

    def pause_listener(self):
        """Pause the alert listener."""
        if not self.listening:
            return 'Alert listener already pasued'

        self.log.info('Pausing alert listener')
        self.listening = False
        return 'Alert listener paused'

    def resume_listener(self):
        """Pause the alert listener."""
        if self.listening:
            return 'Alert listener already running'

        self.log.info('Resuming alert listener')
        self.listening = True
        return 'Alert listener resumed'


if __name__ == '__main__':
    daemon_id = 'sentinel'
    with misc.make_pid_file(daemon_id):
        SentinelDaemon()._run()
