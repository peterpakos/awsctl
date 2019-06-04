# -*- coding: utf-8 -*-
"""This module implements an abstract class loading a more specific cloud class.

Author: Peter Pakos <peter.pakos@wandisco.com>

Copyright (C) 2019 WANdisco
"""

from __future__ import print_function
import os
import abc
import prettytable
from string import Template
from ppmail import Mailer
from CONFIG import CONFIG
import logging

log = logging.getLogger('cloud_tools')


class WDCloud(object):
    VERSION = '1.2.2'
    __metaclass__ = abc.ABCMeta

    def __init__(self, cloud_provider, profile_name):

        cloud_names = {
            'aws': 'AWS',
            'gcp': 'GCP',
            'azure': 'Azure'
        }

        self._cloud_name = cloud_names[cloud_provider]
        self._profile_name = profile_name
        self._regions = []
        try:
            self._mailer = Mailer(slack=True)
        except Exception as e:
            log.critical(e)
            exit(1)

        self._bp_url = {
            'AWS': 'https://workspace.wandisco.com/display/IT/AWS+Best+Practices+at+WANdisco',
            'GCP': 'https://workspace.wandisco.com/display/IT/GCP+Best+Practices+at+WANdisco',
            'Azure': 'https://workspace.wandisco.com/display/IT/Azure+Best+Practices+at+WANdisco'
        }

    @staticmethod
    def loader(cloud_provider, profile_name):
        module = __import__('wd' + cloud_provider)
        return getattr(module, cloud_provider.upper())(cloud_provider, profile_name)

    @abc.abstractmethod
    def list(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def tag(self, *args, **kwargs):
        pass

    def exclude(self, instance_id, *args, **kwargs):
        self.tag(instance_id=instance_id, key='EXCLUDE', value='True', *args, **kwargs)

    def include(self, instance_id, *args, **kwargs):
        self.tag(instance_id=instance_id, key='EXCLUDE', value='False', *args, **kwargs)

    def list_regions(self, disable_border, disable_header, *args, **kwargs):
        table = prettytable.PrettyTable(['Region'], border=not disable_border, header=not disable_header,
                                        sortby='Region')
        table.align = 'l'
        for region in self._regions:
            table.add_row([region])
        print(table)

    @staticmethod
    def _get_uptime(seconds):
        y = divmod(seconds, 86400*364)
        w = divmod(y[1], 86400*7)
        d = divmod(w[1], 86400)
        h = divmod(d[1], 3600)
        m = divmod(h[1], 60)
        s = m[1]
        uptime = []
        if y[0] > 0:
            uptime.append('%dy' % y[0])
        if w[0] > 0:
            uptime.append('%dw' % w[0])
        if d[0] > 0:
            uptime.append('%dd' % d[0])
        if h[0] > 0:
            uptime.append('%dh' % h[0])
        if m[0] > 0:
            uptime.append('%dm' % m[0])
        uptime.append('%ds' % s)
        uptime = ' '.join(uptime)
        return uptime

    @staticmethod
    def _date_diff(date1, date2):
        diff = (date1 - date2)
        diff = (diff.microseconds + (diff.seconds + diff.days * 24 * 3600) * 10 ** 6) / 10 ** 6
        return diff

    def _send_alert(self, mail_type, user, region_ids, name_dict, uptime_dict, warning_threshold, critical_threshold,
                    stop=False, dept=None, rg_dict=None, resource='instance'):
        user_name = user.split('.')[0].capitalize()
        profiles = dept if dept else [self._profile_name.upper()]

        number = 0

        if self._cloud_name == 'Azure':
            table = prettytable.PrettyTable(['Region', 'RG', 'Name', 'Uptime'])
            for region, ids in region_ids.items():
                number += len(ids)
                for iid in ids:
                    table.add_row([
                        region,
                        rg_dict[iid],
                        name_dict[iid],
                        uptime_dict[iid]
                    ])
        else:
            table = prettytable.PrettyTable(['Region', 'Instance ID', 'Name', 'Uptime'])
            for region, ids in region_ids.items():
                number += len(ids)
                for iid in ids:
                    table.add_row([
                        region,
                        iid,
                        name_dict[iid],
                        uptime_dict[iid]
                    ])
        table.align = 'l'

        if number > 1:
            s = 's'
            have = 'have'
        else:
            s = ''
            have = 'has'

        if len(profiles) > 1:
            ss = 's'
        else:
            ss = ''

        if number > 1:
            some_of_them = 'and either all or some of them'
        else:
            some_of_them = 'that'

        sender = CONFIG.EMAIL_FROM

        if user.endswith('.api'):
            user = user.replace('.api', '')

        recipient = user + '@' + CONFIG.EMAIL_DOMAIN

        cc_recipient = []
        if mail_type in ['warning', 'critical']:
            for profile in profiles:
                profile = profile.partition('-')[0].lower()
                if profile in CONFIG.HEADS:
                    for head in CONFIG.HEADS[profile]:
                        if head not in cc_recipient:
                            cc_recipient.append(head)
            if recipient in cc_recipient:
                cc_recipient.remove(recipient)

        if cc_recipient:
            cc = ' (cc: %s)' % ', '.join(cc_recipient)
        else:
            cc = ''

        subject = '%s %s %s: running %ss' % (mail_type.upper(), self._cloud_name, '/'.join(profiles), resource)

        stop_term = 'STOPPED' if resource == 'instance' else 'DELETED'

        if stop:
            stop_msg = '\nANY RESOURCES RUNNING FOR LONGER THAN %s HOURS WILL BE %s IMMEDIATELY!\
        \n\nPlease check your %s account and make sure there are no more offending resources.\n' % \
                       (critical_threshold, stop_term, self._cloud_name)
        else:
            stop_msg = '\nPLEASE IMMEDIATELY STOP OR TERMINATE ANY RESOURCES THAT ARE NO LONGER IN USE!\n'

        template = open('%s/templates/%s.txt' % (os.path.dirname(os.path.realpath(__file__)), mail_type))
        template = Template(template.read())
        message = template.substitute({
            'user_name': user_name,
            'number': number,
            's': s,
            'cloud': self._cloud_name,
            'profile': '/'.join(profiles),
            'ss': ss,
            'table': table,
            'warning_threshold': warning_threshold,
            'critical_threshold': critical_threshold,
            'bp_url': self._bp_url[self._cloud_name],
            'some_of_them': some_of_them,
            'stop_msg': stop_msg,
            'have': have,
            'resource': resource,
            'stop_term': stop_term
        })

        print('Sending %s notification to %s%s... ' % (mail_type, recipient, cc), end='')
        response = self._mailer.send(
            sender=sender,
            recipients=recipient,
            subject=subject,
            message=message,
            code=True,
            cc=cc_recipient
        )
        if response:
            print('SUCCESS')
        else:
            print('FAIL')

    def _check_region(self, region):
        if region not in self._regions:
            print('Region must be one of the following:\n- %s' %
                  '\n- '.join(self._regions))
            exit(1)
        else:
            self._regions = [region]

    @abc.abstractmethod
    def sg(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def public_buckets(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def create_image(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def stop(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def start(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def terminate(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def list_hdi(self, *args, **kwargs):
        pass

    @staticmethod
    def _ip_sum(ip, n):
        import socket
        import struct
        unpacked = struct.unpack('!L', socket.inet_aton(ip))[0]
        packed = socket.inet_ntoa(struct.pack('!L', unpacked + n))
        return packed
