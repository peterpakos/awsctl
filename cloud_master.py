#!/usr/bin/env python
#
# A tool to manipulate clouds.
#
# Author: Peter Pakos <peter.pakos@wandisco.com>

from __future__ import print_function
from sys import stderr, argv
from os import path
from argparse import ArgumentParser
from boto3 import Session
from botocore import exceptions
from prettytable import PrettyTable


class Main(object):
    _version = '16.3.17'
    _name = path.basename(argv[0])
    _profile_name = None
    _cloud_provider = None
    _cloud = None

    def __init__(self):
        action = self.parse_args()
        self._cloud = Cloud.loader(self._cloud_provider, self._profile_name)
        if action == 'list':
            self._cloud.display_list()
        else:
            self.die('Action %s not implemented yet' % action)

    def _del_(self):
        pass

    def display_version(self):
        print('%s version %s' % (self._name, self._version))

    @staticmethod
    def die(message=None, code=1):
        if message is not None:
            print(message, file=stderr)
        exit(code)

    def parse_args(self):
        parser = ArgumentParser(description='A tool to manipulate clouds.')
        parser.add_argument('-v', '--version',
                            help='show version', action='store_true')
        parser.add_argument('action', help='operation mode', nargs='?', choices=['list'])
        parser.add_argument('-c', '--cloud-provider', help='Cloud provider (default: %(default)s)',
                            dest='cloud_provider', choices=['aws'], default='aws')
        parser.add_argument('-p', '--profile-name', help='Cloud profile name (default: %(default)s)',
                            dest='profile_name', default='default')
        args = parser.parse_args()
        if args.version:
            self.display_version()
            exit()
        if not args.action:
            parser.print_usage()
            exit()
        self._profile_name = args.profile_name
        self._cloud_provider = args.cloud_provider
        return args.action


class Cloud(object):
    def __init__(self, cloud_provider, profile_name):
        self._cloud_provider = cloud_provider
        self._profile_name = profile_name

    @staticmethod
    def loader(cloud_provider, profile_name):
        classes = {'aws': AWS, 'azure': AZURE, 'gce': GCE}
        return classes[cloud_provider](cloud_provider, profile_name)


class AWS(Cloud):
    def __init__(self, cloud_provider, profile_name):
        super(AWS, self).__init__(cloud_provider, profile_name)

    def display_list(self):
        s = None
        try:
            s = Session(profile_name=self._profile_name)
        except exceptions.ProfileNotFound as err:
            Main.die(err.message)
        ec2 = s.resource('ec2')
        instances = ec2.instances.all()
        table = PrettyTable(['ID', 'Type', 'State', 'Launch time', 'Key name'])
        for instance in instances:
            table.add_row([instance.id, instance.instance_type, instance.state['Name'], instance.launch_time,
                           instance.key_name])
        print(table)


class AZURE(Cloud):
    def __init__(self, cloud_provider, profile_name):
        super(AZURE, self).__init__(cloud_provider, profile_name)
        Main.die('%s cloud not implemented yet, exiting...' % self._cloud_provider.upper())


class GCE(Cloud):
    def __init__(self, cloud_provider, profile_name):
        super(GCE, self).__init__(cloud_provider, profile_name)
        Main.die('%s cloud not implemented yet, exiting...' % self._cloud_provider.upper())


if __name__ == '__main__':
    app = Main()
