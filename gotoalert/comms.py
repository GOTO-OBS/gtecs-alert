#! /opt/local/bin/python3.6

import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from slacker import Slacker


def send_email(fromaddr, toaddr, subject, body, password, file_path, file_name):
    """Send an email when an event is detected."""
    # Create message
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Attach HTML file
    html_file = file_name + '.html'
    with open(file_path + html_file, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename={}'.format(html_file))
    msg.attach(part)

    # Connect to server and send
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, password)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def send_slackmessage(text, time, ra, dec, file_name):
    slack_client = Slacker("xoxb-132218163666-416316276000-iueLeJ9b6JKToTTMGDvK5XaN")

    message = '{}  (Time = {})  (RA = {})  (DEC = {}) '.format(text, time, ra, dec)
    html_file = file_name + '.html'
    link = 'http://118.138.235.166/~obrads/Transients_For_La_Palma_Observatory/' + html_file
    message += 'See more at ' + link

    slack_client.chat.post_message('#grb-alerts', message)
