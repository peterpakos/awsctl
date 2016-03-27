#!/usr/bin/env python
#
# A tool to send mail via sendgrid
#
# Author: Peter Pakos <peter.pakos@wandisco.com>

from __future__ import print_function
import sys
import os
import argparse
import platform
import sendgrid
import json


class Main(object):
    _version = '16.3.27'
    _name = os.path.basename(sys.argv[0])

    def __init__(self):
        args = self._parse_args()
        if args.sender:
            sender = args.sender
        else:
            sender = os.getenv('USER') + '@' + platform.node()
        message = ''
        for line in sys.stdin:
            message += line
        mail = Mail()
        status, msg = mail.send(sender, args.recipient, args.subject, message, args.html)
        if status == 200:
            exit()
        else:
            print('\nError: %s' % msg['errors'][0])
            exit(status)

    def _parse_args(self):
        parser = argparse.ArgumentParser(description='A tool to send mail via sendgrid')
        parser.add_argument('-v', '--version',
                            help='show version', action='store_true', dest='version')
        parser.add_argument('-f', '--from', dest='sender',
                            help='email From: field')
        parser.add_argument('-t', '--to', dest='recipient', nargs='+', required=True,
                            help='email To: field')
        parser.add_argument('-s', '--subject', dest='subject', required=True,
                            help='email Subject: field')
        parser.add_argument('-H', '--html', dest='html', action='store_true',
                            help='send HTML formatted email')
        args = parser.parse_args()
        if args.version:
            self._display_version()
            exit()
        return args

    def _display_version(self):
        print('%s version %s' % (self._name, self._version))


class Mail(object):

    def __init__(self):
        self._cwd = os.path.dirname(os.path.abspath(sys.argv[0]))
        self._api_file = self._cwd + '/wdmailer.api'
        self._api_user = None
        self._api_key = None
        if os.path.isfile(self._api_file):
            f = open(self._api_file)
            self._api_user = f.readline().strip()
            self._api_key = f.readline().strip()
        if self._api_user is None or self._api_key is None:
            print('API credentials not found in file %s' % self._api_file)
            exit(1)
        self._sg = sendgrid.SendGridClient(self._api_user, self._api_key)

    def send(self, sender, recipient, subject, message, html=False):
        mail = sendgrid.Mail()
        body = ''
        if html:
            body += '''
<html>
<body>
<pre>
'''
        body += message

        if html:
            body += '''
</pre>
</body>
</html>
'''
        mail.set_from(sender)
        mail.add_to(recipient)
        mail.set_subject(subject)
        if html:
            mail.set_html(body)
        else:
            mail.set_text(body)
        status, msg = self._sg.send(mail)
        return status, json.loads(msg)


if __name__ == '__main__':
    try:
        main = Main()
    except KeyboardInterrupt:
        print('\nCancelling...')
