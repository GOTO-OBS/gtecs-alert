#! /opt/local/bin/python3.6

from slacker import Slacker
from slackclient import SlackClient


def slackmessage(text, time, ra, dec, file):
    slackClient = Slacker("xoxb-132218163666-416316276000-iueLeJ9b6JKToTTMGDvK5XaN")

    messageToChannel = text+"  (Time = "+time+")"+"  (RA = "+ra+")"+"  (DEC = "+dec+") See more at http://118.138.235.166/~obrads/Transients_For_La_Palma_Observatory/"+file+".html"

    slackClient.chat.post_message("#grb-alerts",messageToChannel)
