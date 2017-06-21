# -*- coding: utf-8 -*-
"""This module implements sending mail via Sendgrid.

Configuration:
    * Save Sendgrid API key to WDMail.api file in the project's root directory

Copyright (C) 2017 Peter Pakos <peter.pakos@wandisco.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from __future__ import print_function
import sendgrid
import urllib2
import os
import cgi


class WDMail(object):
    VERSION = '1.0.0'

    def __init__(self):
        self._cwd = os.path.dirname(os.path.realpath(__file__))
        self._name = os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]
        self._api_file = '%s/%s.api' % (self._cwd, self._name)
        self._api_key = None

        if os.path.isfile(self._api_file):
            f = open(self._api_file)
            self._api_key = f.readline().strip()
        if self._api_key is None:
            print('Sendgrid API key not found in file %s' % self._api_file)
            exit(1)
        os.environ['SENDGRID_API_KEY'] = self._api_key
        self._sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))

    def send(self, sender, recipients, subject, message, html=False, cc=None, font_size=None):
        if html:
            message = cgi.escape(message)

        if font_size:
            style = ' style="font-size:%spx"' % font_size
        else:
            style = ''

        if type(recipients) is not list:
            recipientsl = []
            if recipients is not None:
                recipientsl.append(recipients)
            recipients = recipientsl

        if type(cc) is not list:
            ccl = []
            if cc is not None:
                ccl.append(cc)
            cc = ccl

        content = ''
        content_type = 'text/plain'
        if html:
            content_type = 'text/html'
            content += '''
<html>
<body%s>
<pre>
''' % style
        content += message

        if html:
            content += '''
</pre>
</body>
</html>
'''
        recipient_list = []
        for recipient in recipients:
            if recipient in cc:
                cc.remove(recipient)
            recipient_list.append({"email": recipient})
        data = {
            'personalizations': [
                {
                    'to': recipient_list,
                    'subject': subject
                }
            ],
            'from': {
                'email': sender
            },
            'content': [
                {
                    'type': content_type,
                    'value': content
                }
            ]
        }
        if len(cc) > 0:
            cc_list = []
            for c in cc:
                cc_list.append({'email': c})
            data['personalizations'][0]['cc'] = cc_list
        response = None
        try:
            response = self._sg.client.mail.send.post(request_body=data)
        except urllib2.HTTPError as err:
            print(err)
            exit(1)

        return response.status_code
