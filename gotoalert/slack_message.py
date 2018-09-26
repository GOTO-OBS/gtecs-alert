#! /opt/local/bin/python3.6

from slacker import Slacker


def slackmessage(text, time, ra, dec, file_name):
    slack_client = Slacker("xoxb-132218163666-416316276000-iueLeJ9b6JKToTTMGDvK5XaN")

    message = '{}  (Time = {})  (RA = {})  (DEC = {}) '.format(text, time, ra, dec)
    html_file = file_name + '.html'
    link = 'http://118.138.235.166/~obrads/Transients_For_La_Palma_Observatory/' + html_file
    message += 'See more at ' + link

    slack_client.chat.post_message('#grb-alerts', message)
