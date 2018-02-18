# -*- coding: utf-8 -*-
"""This module provides GCP class.

Author: Peter Pakos <peter.pakos@wandisco.com>

Copyright (C) 2019 WANdisco
"""

from __future__ import print_function
import os
import datetime
import prettytable
import tzlocal
from oauth2client.client import GoogleCredentials, HttpAccessTokenRefreshError
from googleapiclient import discovery, errors
import iso8601

from CONFIG import CONFIG
import logging
import time
from wdcloud import WDCloud

log = logging.getLogger('cloud_tools')


class GCP(WDCloud):
    def __init__(self, *args, **kwargs):
        super(GCP, self).__init__(*args, **kwargs)
        self._zones = []
        self._project = 'fusion-gce-testing' if str(self._profile_name).lower() == 'old'\
            else CONFIG.GCP_PROJECT_PREFIX + str(self._profile_name).lower()
        if self._profile_name != 'default':
            credentials_file = os.path.dirname(__file__) + '/' + str(self._profile_name).upper() + '.json'
            if os.path.isfile(credentials_file):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_file
            else:
                print('Credentials file %s does not exist.' % credentials_file)
                exit(1)
        credentials = GoogleCredentials.get_application_default()
        self._compute = discovery.build('compute', 'v1', credentials=credentials)
        zones = None
        try:
            zones = self._compute.zones().list(project=self._project).execute()
        except HttpAccessTokenRefreshError as e:
            print('Auth Error (%s)' % e)
            exit(1)

        for zone in zones['items']:
            self._zones.append(zone['name'])
            region = str(zone['name']).rsplit('-', 1)[0]
            if region not in self._regions:
                self._regions.append(region)

    @staticmethod
    def _operations_get(operations, instance_id, resource):
        if type(operations) is not list:
            return None
        for operation in operations:
            if operation.get('targetId') == instance_id and operation.get('status') == 'DONE'\
                    and operation.get('operationType') in ['insert', 'start']:
                return operation.get(resource)
        return None

    def list(self, disable_border=False, disable_header=False, state=None, notify=False, stop=False,
             warning_threshold=None, critical_threshold=None, tag=None, *args, **kwargs):
        if not state:
            state = ['running', 'staging', 'provisioning', 'stopping', 'terminated']
        table = prettytable.PrettyTable(['Region', 'Name', 'Type', 'Image', 'State', 'Creation time',
                                         'Launch time', 'Uptime', 'User', 'Private IP', 'Public IP',
                                         'Exclude'],
                                        border=not disable_border, header=not disable_header, reversesort=True,
                                        sortby='Creation time')
        table.align = 'l'
        i = 0
        states_dict = {}
        uptime_dict = {}
        name_dict = {}
        stop_dict = {}
        info_dict = {}
        warning_dict = {}
        critical_dict = {}
        local_tz = tzlocal.get_localzone()
        now = local_tz.localize(datetime.datetime.now())
        for zone in self._zones:
            region = str(zone).rsplit('-', 1)[0]
            if region not in self._regions:
                continue
            instances = self._compute.instances().list(project=self._project, zone=zone).execute()
            if not instances.get('items'):
                continue
            operations = self._compute.zoneOperations().list(project=self._project, zone=zone,
                                                             orderBy='creationTimestamp desc').execute().get('items')
            for instance in instances.get('items'):
                instance_id = instance.get('id')
                instance_state = str(instance.get('status')).lower()
                if instance_state not in state:
                    continue
                creation_time = instance.get('creationTimestamp')
                creation_time = iso8601.parse_date(creation_time).astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S') \
                    if creation_time else ''
                instance_name = instance.get('name')
                instance_type = str(instance.get('machineType')).rsplit('/', 1)[1]
                image_name = str(instance.get('disks')[0]['licenses'][0]).rsplit('/', 1)[1]
                private_ip_address = instance.get('networkInterfaces')[0].get('networkIP')
                public_ip_address = instance.get('networkInterfaces')[0]['accessConfigs'][0].get('natIP')
                public_ip_address = public_ip_address or ''
                last_user = self._operations_get(operations, instance_id, 'user')
                last_user = str(last_user).split('@', 1)[0] if last_user else ''
                launch_time = ''
                launch_time_src = self._operations_get(operations, instance_id, 'endTime')
                if launch_time_src:
                    launch_time_src = iso8601.parse_date(launch_time_src).astimezone(local_tz)
                    launch_time = launch_time_src.strftime('%Y-%m-%d %H:%M:%S')
                uptime = ''
                excluded = False

                if instance_state == 'running' and launch_time_src:
                    seconds = self._date_diff(now, launch_time_src)
                    uptime = self._get_uptime(seconds)
                    if seconds >= (critical_threshold * 3600) and not excluded:
                        if region not in stop_dict:
                            stop_dict[region] = []
                        stop_dict[region].append(instance_id)
                    if last_user and notify and not excluded:
                        name_dict[instance_id] = instance_name
                        uptime_dict[instance_id] = uptime
                        if last_user not in info_dict:
                            info_dict[last_user] = {}
                        if region not in info_dict[last_user]:
                            info_dict[last_user][region] = []
                        info_dict[last_user][region].append(instance_id)

                        if seconds >= (critical_threshold * 3600):
                            critical_dict[last_user] = True
                        elif seconds >= (warning_threshold * 3600):
                            warning_dict[last_user] = True
                i += 1
                table.add_row([
                    region,
                    instance_name,
                    instance_type,
                    image_name,
                    instance_state,
                    creation_time,
                    launch_time,
                    uptime,
                    last_user,
                    private_ip_address,
                    public_ip_address,
                    excluded
                ])
                if instance_state in states_dict:
                    states_dict[instance_state] += 1
                else:
                    states_dict[instance_state] = 1
        print(table)
        out = ', '.join(['%s: %s' % (key, value) for (key, value) in sorted(states_dict.items())])
        if len(out) > 0:
            out = '(%s)' % out
        else:
            out = ''
        print('Time: %s (%s) | Instances: %s %s' % (now.strftime('%Y-%m-%d %H:%M:%S'), str(local_tz), i, out))
        if len(info_dict) > 0:
            print()

        for user, region_ids in info_dict.items():
            if user in critical_dict:
                mail_type = 'critical'
            elif user in warning_dict:
                mail_type = 'warning'
            else:
                mail_type = 'info'
            self._send_alert(mail_type,
                             user,
                             region_ids,
                             name_dict,
                             uptime_dict,
                             warning_threshold,
                             critical_threshold,
                             stop)

    def run(self, region, subnet_id, image_id_list, ssh_key, count=1, instance_type=None, private_ip=None,
            volume_size=None, tag=None, user_data=None, name=None, *args, **kwargs):
        zone = region
        region = str(zone).rsplit('-', 1)[0]
        if zone not in self._zones:
            print('Zone must be one of the following:\n- %s' %
                  '\n- '.join(self._zones))
            exit(1)

        s = 's' if count > 1 else ''

        if not name:
            name = 'no_name'

        if not volume_size:
            volume_size = 10

        image_id = image_id_list[0] if len(image_id_list) == 1 else None

        if not instance_type:
            instance_type = 'n1-standard-1'

        creator = os.getenv('BUILD_USER_ID', os.getenv('USER', 'unknown'))

        if len(image_id_list) != count and len(image_id_list) > 1:
            log.critical('Number of images needs to be equal to 1 or instance count (%s)' % count)
            exit(1)

        log.info('Creating %s %s instance%s in region %s as user %s...' %
                 (count, instance_type, s, region, creator))

        operation_list = []
        for i in range(count):
            ip = self._ip_sum(private_ip, i) if private_ip else None

            if len(image_id_list) > 1 and count > 1:
                image_id = image_id_list[i]

            project = None
            if 'centos' in image_id:
                project = 'centos-cloud'
            elif 'ubuntu' in image_id:
                project = 'ubuntu-os-cloud'
            elif 'debian' in image_id:
                project = 'debian-cloud'
            else:
                log.critical('Image %s is not supported' % image_id)
                exit(1)

            image_response = self._compute.images().getFromFamily(project=project, family=image_id).execute()
            source_disk_image = image_response['selfLink']

            config = {
                'name': (name + '-%s' % (i + 1)) if count > 1 else name,
                'machineType': 'zones/%s/machineTypes/%s' % (zone, instance_type),
                'networkInterfaces': [{
                    'subnetwork': 'projects/%s/regions/%s/subnetworks/%s' % (self._project, region, subnet_id),
                    'networkIP': ip,
                    'accessConfigs':
                        [{
                            'name': 'External NAT',
                            'type': 'ONE_TO_ONE_NAT',
                            'networkTier': 'PREMIUM'
                        }],

                }],
                'disks': [{
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                        'diskSizeGb': volume_size
                    }
                }]
            }

            if user_data:
                config['metadata'] = {
                    'items': [
                        {
                            'key': 'startup-script',
                            'value': user_data
                        }
                    ]
                }

            local_ssd_disks = os.getenv('LOCAL_SSD_DISKS')
            if local_ssd_disks and local_ssd_disks.isdigit() and 1 <= int(local_ssd_disks) <= 8:
                for n in range(int(local_ssd_disks)):
                    config['disks'].append({
                        'mode': 'READ_WRITE',
                        'deviceName': 'local-ssd-%s' % n,
                        'type': 'SCRATCH',
                        'autoDelete': True,
                        'interface': 'SCSI',
                        'initializeParams': {
                            'diskType': 'projects/%s/zones/%s/diskTypes/local-ssd' % (self._project, zone)
                        }
                    })

            ssd_disk = os.getenv('SSD_DISK')
            if ssd_disk and ssd_disk.isdigit() and 10 <= int(ssd_disk) <= 2000:
                config['disks'].append({
                    'mode': 'READ_WRITE',
                    'deviceName': 'disk-1',
                    'type': 'PERSISTENT',
                    'autoDelete': True,
                    'initializeParams': {
                        'diskName': 'disk-1',
                        'diskType': 'projects/%s/zones/%s/diskTypes/pd-ssd' % (self._project, zone),
                        'diskSizeGb': ssd_disk
                    }
                })

            deletion_protection = os.getenv('DELETION_PROTECTION')
            if deletion_protection:
                config['deletionProtection'] = True

            operation = None
            try:
                operation = self._compute.instances().insert(project=self._project, zone=zone, body=config).execute()
            except errors.HttpError as e:
                log.critical(e)
                exit(1)

            operation_list.append(operation['name'])

        try:
            for i, operation in enumerate(operation_list, 1):
                log.info('Waiting for instance %s to start...' % i)
                self._wait_for_operation(self._compute, self._project, zone, operation)
        except Exception as e:
            log.critical(e)
            exit(1)

        log.info('All instances are ready.')

    @staticmethod
    def _wait_for_operation(compute, project, zone, operation):
        while True:
            result = compute.zoneOperations().get(
                project=project,
                zone=zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)

    def sg(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def public_buckets(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def tag(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def create_image(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def terminate(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def stop(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def start(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def list_hdi(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)
