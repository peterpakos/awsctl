#!/usr/bin/env python
#
# Script for manipulating AWS account.
#
# Author: Peter Pakos <peter.pakos@wandisco.com>

from __future__ import print_function
from sys import stderr, argv
from os import path
from getopt import getopt, GetoptError
from boto3 import Session


class Main(object):
    __version = '16.3.15'
    __name = path.basename(argv[0])

    def __init__(self):
        self.parse_options()
        exit()
        s = Session(profile_name='wandisco')
        ec2 = s.resource('ec2')
        ec2c = s.client('ec2')
        print(ec2c.describe_availability_zones())
        instances = ec2.instances.all()
        for instance in instances:
            print(instance.id, instance.instance_type, instance.state['Name'], instance.launch_time,
                  instance.hypervisor, instance.key_name)

    def __del__(self):
        return True

    def parse_options(self):

        if len(argv) == 1:
            self.display_usage()
            exit()

        if argv[1] in ['-h', '--help', 'help']:
            self.display_usage()
            exit()

        elif argv[1] in ['-v', '--version', 'version']:
            self.display_version()
            exit()

        elif argv[1] == 'list':
            options = None

            try:
                options, args = getopt(argv[2:], 'h', [
                    'help',
                ])
            except GetoptError as err:
                self.die(err)

            for opt, arg in options:
                if opt in ('-h', '--help'):
                    self.display_usage()
                    exit()

        else:
            self.die('Unrecognised option: %s' % argv[1])

    def display_version(self):
        print('%s %s (https://github.com/peterpakos)' % (self.__name, self.__version))

    def display_usage(self):
        self.display_version()
        print('''Usage: %s [OPTIONS]
AVAILABLE OPTIONS:
-h, --help      Print this help summary page
-v, --version   Print version number''' % self.__name)

    @staticmethod
    def die(message=None, code=1):
        if message is not None:
            print(message, file=stderr)
        exit(code)


if __name__ == '__main__':
    app = Main()
