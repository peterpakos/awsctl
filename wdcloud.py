# -*- coding: utf-8 -*-
"""This module implements interaction with cloud providers.

Author: Peter Pakos <peter.pakos@wandisco.com>

Copyright (C) 2017 WANdisco

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
import os
import sys
import abc
import datetime
import boto3
import botocore.exceptions
import prettytable
import tzlocal
from string import Template
from oauth2client.client import GoogleCredentials, HttpAccessTokenRefreshError
from googleapiclient import discovery
import iso8601
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.subscriptions import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.monitor import MonitorClient
from msrestazure.azure_exceptions import CloudError
from ppmail import Mailer
from CONFIG import CONFIG


class WDCloud(object):
    VERSION = '1.1.0'
    __metaclass__ = abc.ABCMeta

    def __init__(self, cloud_provider, profile_name, region):

        self._cloud_names = {
            'aws': 'AWS',
            'gcp': 'GCP',
            'azure': 'Azure'
        }

        self._cloud_provider = self._cloud_names[cloud_provider]
        self._profile_name = profile_name
        self._region = region
        self._regions = []
        self._mailer = Mailer(slack=True)

        self._bp_url = {
            'AWS': 'https://workspace.wandisco.com/display/IT/AWS+Best+Practices+at+WANdisco',
            'GCP': 'https://workspace.wandisco.com/display/IT/GCP+Best+Practices+at+WANdisco',
            'Azure': 'https://workspace.wandisco.com/display/IT/Azure+Best+Practices+at+WANdisco'
        }

    @staticmethod
    def loader(cloud_provider, profile_name, region):
        classes = {'aws': AWS, 'azure': Azure, 'gcp': GCP}
        return classes[cloud_provider](cloud_provider, profile_name, region)

    @abc.abstractmethod
    def list(self):
        pass

    def list_regions(self, disable_border=False, disable_header=False):
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
                    stop=False, dept=None, rg_dict=None):
        user_name = user.split('.')[0].capitalize()
        profiles = dept if dept else [self._profile_name.upper()]

        number = 0

        if self._cloud_provider == 'Azure':
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
        recipient = user + '@' + CONFIG.EMAIL_DOMAIN

        cc_recipient = []
        if mail_type in ['warning', 'critical']:
            for profile in profiles:
                if profile.lower() in CONFIG.HEADS:
                    cc_recipient += CONFIG.HEADS[profile.lower()]
            if recipient in cc_recipient:
                cc_recipient.remove(recipient)

        if cc_recipient:
            cc = ' (cc: %s)' % ', '.join(cc_recipient)
        else:
            cc = ''

        subject = '%s %s %s: running instances' % (mail_type.upper(), self._cloud_provider, '/'.join(profiles))

        if stop:
            stop_msg = '\nANY INSTANCES RUNNING FOR LONGER THAN %s HOURS WILL BE STOPPED IMMEDIATELY!\
        \n\nPlease check your %s account and make sure there are no more offending instances.\n' % \
                       ((critical_threshold / 3600), self._cloud_provider)
        else:
            stop_msg = '\nPLEASE IMMEDIATELY STOP OR TERMINATE ANY INSTANCES THAT ARE NO LONGER IN USE!\n'

        template = open('%s/templates/%s.txt' % (os.path.dirname(os.path.realpath(__file__)), mail_type))
        template = Template(template.read())
        message = template.substitute({
            'user_name': user_name,
            'number': number,
            's': s,
            'cloud': self._cloud_provider,
            'profile': '/'.join(profiles),
            'ss': ss,
            'table': table,
            'warning_threshold': warning_threshold / 3600,
            'critical_threshold': critical_threshold / 3600,
            'bp_url': self._bp_url[self._cloud_provider],
            'some_of_them': some_of_them,
            'stop_msg': stop_msg,
            'have': have,
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
            print('FAILURE')

    def _check_region(self):
        if self._region:
            if self._region not in self._regions:
                print('Region must be one of the following:\n- %s' %
                      '\n- '.join(self._regions))
                exit(1)
            else:
                self._regions = [self._region]

    @abc.abstractmethod
    def sg(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def public_buckets(self, *args, **kwargs):
        pass


class AWS(WDCloud):
    def __init__(self, *args, **kwargs):
        super(AWS, self).__init__(*args, **kwargs)
        self._session = None
        ec2c = None
        try:
            self._session = boto3.Session(profile_name=self._profile_name)
        except botocore.exceptions.ProfileNotFound as err:
            print(err)
            exit(1)
        try:
            ec2c = self._session.client('ec2')
        except botocore.exceptions.NoRegionError as err:
            print(err)
            exit(1)
        regions = None
        try:
            regions = ec2c.describe_regions()
        except botocore.exceptions.EndpointConnectionError as err:
            print(err)
            exit(1)
        except botocore.exceptions.ClientError as err:
            print(err)
            exit(1)

        for region in regions['Regions']:
            self._regions.append(region['RegionName'])

        self._check_region()

    @staticmethod
    def _get_tag(list_a, search_key):
        value = None
        if type(list_a) == list:
            for item in list_a:
                if item['Key'] == search_key:
                    value = item['Value']
                    break
        return value

    def list(self, disable_border=False, disable_header=False, state=None, notify=False, stop=False,
             warning_threshold=None, critical_threshold=None):
        if not state:
            state = ['running', 'pending', 'shutting-down', 'stopped', 'stopping', 'terminated']
        table = prettytable.PrettyTable(['Zone', 'ID', 'Name', 'Type', 'Image', 'State',
                                         'Launch time', 'Uptime', 'User', 'SSH key', 'Private IP', 'Public IP',
                                         'Exclude'],
                                        border=not disable_border, header=not disable_header, reversesort=True,
                                        sortby='Launch time')
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
        for region in self._regions:
            ec2r = self._session.resource('ec2', region_name=region)
            instances = ec2r.instances.filter(Filters=[{
                'Name': 'instance-state-name',
                'Values': state
            }])
            for instance in instances:
                i += 1
                excluded = True if self._get_tag(instance.tags, 'EXCLUDE') else False
                image_name = ''
                private_ip_address = instance.private_ip_address or ''
                public_ip_address = instance.public_ip_address or ''
                instance_state = instance.state['Name']
                last_user = self._get_tag(instance.tags, 'Last_user') or ''
                uptime = ''
                name = self._get_tag(instance.tags, 'Name')
                if name is None:
                    name = ''
                then = instance.launch_time.astimezone(local_tz)
                launch_time = str(then).partition('+')[0]
                if instance_state == 'running':
                    seconds = self._date_diff(now, then)
                    uptime = self._get_uptime(seconds)

                    if seconds >= critical_threshold and not excluded:
                        if region not in stop_dict:
                            stop_dict[region] = []
                        stop_dict[region].append(instance.id)

                    if last_user and notify and not excluded:
                        name_dict[instance.id] = name
                        uptime_dict[instance.id] = uptime
                        if last_user not in info_dict:
                            info_dict[last_user] = {}
                        if region not in info_dict[last_user]:
                            info_dict[last_user][region] = []
                        info_dict[last_user][region].append(instance.id)

                        if seconds >= critical_threshold:
                            critical_dict[last_user] = True
                        elif seconds >= warning_threshold:
                            warning_dict[last_user] = True

                try:
                    image_name = instance.image.name[0:15]
                except AttributeError:
                    pass
                table.add_row([
                    instance.placement['AvailabilityZone'],
                    instance.id,
                    name,
                    instance.instance_type,
                    image_name,
                    instance_state,
                    launch_time,
                    uptime,
                    last_user,
                    instance.key_name,
                    private_ip_address,
                    public_ip_address,
                    excluded
                ])
                if instance.state['Name'] in states_dict:
                    states_dict[instance.state['Name']] += 1
                else:
                    states_dict[instance.state['Name']] = 1
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

        if stop and len(stop_dict) > 0:
            for region, iids in stop_dict.items():
                print('\nStopping instances in region %s (%s)... %s' % (
                    region,
                    ','.join(iids),
                    'SUCCESS' if self._stop_instance(region, iids) else 'FAIL')
                      )

    def _stop_instance(self, region, instance_ids):
        ec2r = self._session.resource('ec2', region_name=region)
        response = ec2r.instances.filter(InstanceIds=instance_ids).stop()
        if response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def _create_tag(self, region, resource, key, value):
        ec2c = self._session.client('ec2', region_name=region)
        ec2c.create_tags(Resources=[resource], Tags=[{
            'Key': key,
            'Value': value
        }])
        return True

    def _delete_tag(self, region, resource, key):
        ec2c = self._session.client('ec2', region_name=region)
        ec2c.delete_tags(Resources=[resource], Tags=[{
            'Key': key
        }])
        return True

    def tag(self, instance_id, key, value='', delete=False):
        i = 0
        for region in self._regions:
            ec2r = self._session.resource('ec2', region_name=region)
            instances = ec2r.instances.filter(Filters=[{
                'Name': 'instance-state-name',
                'Values': ['running', 'pending', 'shutting-down', 'stopped', 'stopping', 'terminated']
            }])
            for instance in instances:
                if instance.id in instance_id:
                    i += 1
                    if delete:
                        print('Instance ID %s found in region %s, deleting tag \'%s\': ' % (instance.id, region, key),
                              end='')
                        response = self._delete_tag(region=region, resource=instance.id, key=key)
                    else:
                        print('Instance ID %s found in region %s, creating tag \'%s\': ' % (instance.id, region, key),
                              end='')
                        response = self._create_tag(region=region, resource=instance.id, key=key, value=value)
                    if response:
                        print('OK')
                    else:
                        print('FAIL')
                    if len(instance_id) == i:
                        return
        if i == 0:
            if self._region:
                region = 'region ' + self._region
            else:
                region = 'any region'
            print('Instance ID %s not found in %s' % (', '.join(instance_id), region))

    def sg(self, cidr, delete=False):
        if delete:
            action = 'Deleting'
            tofrom = 'from'
        else:
            action = 'Adding'
            tofrom = 'to'
        print('%s source %s %s Security Groups inbound rules...' % (action, cidr, tofrom))

        for region in self._regions:
            ec2r = self._session.resource('ec2', region_name=region)
            security_groups = ec2r.security_groups.all()
            num = len(list(security_groups.all()))
            i = 0
            for sg in security_groups:
                i += 1
                print('\nSECURITY GROUP: %s (Region %s: %s/%s)' % (sg.id, region, i, num))
                try:
                    print('ALL TRAFFIC: ', end='')
                    if delete:
                        sg.revoke_ingress(
                            IpProtocol='-1',
                            CidrIp=cidr
                        )
                    else:
                        sg.authorize_ingress(
                            IpProtocol='-1',
                            CidrIp=cidr
                        )
                except botocore.exceptions.ClientError as err:
                    print(err)
                    try:
                        print('TCP: ', end='')
                        if delete:
                            sg.revoke_ingress(
                                IpProtocol='tcp',
                                FromPort=0,
                                ToPort=65535,
                                CidrIp=cidr
                            )
                        else:
                            sg.authorize_ingress(
                                IpProtocol='tcp',
                                FromPort=0,
                                ToPort=65535,
                                CidrIp=cidr
                            )
                    except botocore.exceptions.ClientError as err:
                        print(err)
                    else:
                        print('OK')
                    try:
                        print('UDP: ', end='')
                        if delete:
                            sg.revoke_ingress(
                                IpProtocol='udp',
                                FromPort=0,
                                ToPort=65535,
                                CidrIp=cidr
                            )
                        else:
                            sg.authorize_ingress(
                                IpProtocol='udp',
                                FromPort=0,
                                ToPort=65535,
                                CidrIp=cidr
                            )
                    except botocore.exceptions.ClientError as err:
                        print(err)
                    else:
                        print('OK')
                    try:
                        print('ICMP: ', end='')
                        if delete:
                            sg.revoke_ingress(
                                IpProtocol='icmp',
                                FromPort=-1,
                                ToPort=-1,
                                CidrIp=cidr
                            )
                        else:
                            sg.authorize_ingress(
                                IpProtocol='icmp',
                                FromPort=-1,
                                ToPort=-1,
                                CidrIp=cidr
                            )
                    except botocore.exceptions.ClientError as err:
                        print(err)
                    else:
                        print('OK')
                else:
                    print('OK')

    def public_buckets(self, disable_border=False, disable_header=False):
        s3c = self._session.client('s3')

        table = prettytable.PrettyTable(['Public S3 bucket', 'ACL'],
                                        border=not disable_border, header=not disable_header, reversesort=False,
                                        sortby='Public S3 bucket')
        table.align = 'l'

        public_acl_indicator = 'http://acs.amazonaws.com/groups/global/AllUsers'
        public_buckets = {}

        list_bucket_response = None
        try:
            list_bucket_response = s3c.list_buckets()
        except botocore.exceptions.ClientError as e:
            print(e)
            exit(1)

        for bucket_dict in list_bucket_response.get('Buckets'):
            bucket = bucket_dict.get('Name')
            bucket_acl_response = None
            try:
                bucket_acl_response = s3c.get_bucket_acl(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                print(e)
                exit(1)

            for grant in bucket_acl_response.get('Grants'):
                for (k, v) in grant.items():
                    if k == 'Permission' and grant.get('Grantee').get('URI') == public_acl_indicator:
                        if bucket_dict.get('Name') not in public_buckets:
                            public_buckets[bucket] = [v]
                        else:
                            if v not in public_buckets[bucket_dict.get('Name')]:
                                public_buckets[bucket].append(v)

        if public_buckets:
            for bucket, acl in public_buckets.items():
                table.add_row([bucket, ', '.join(acl)])

        print(table)
        print('[%s] Public buckets: %s/%s' %
              (self._profile_name, len(public_buckets), len(list_bucket_response.get('Buckets'))))


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

        self._check_region()

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
             warning_threshold=None, critical_threshold=None):
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
                    if seconds >= critical_threshold and not excluded:
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

                        if seconds >= critical_threshold:
                            critical_dict[last_user] = True
                        elif seconds >= warning_threshold:
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

    def sg(self, *args, **kwargs):
        print('Command not implemented yet', file=sys.stderr)
        exit(1)

    def public_buckets(self, *args, **kwargs):
        print('Command not implemented yet', file=sys.stderr)
        exit(1)


class Azure(WDCloud):
    def __init__(self, *args, **kwargs):
        super(Azure, self).__init__(*args, **kwargs)

        self._subscription_id = CONFIG.AZURE_SUBSCRIPTION_ID
        self._credentials = ServicePrincipalCredentials(
            client_id=CONFIG.AZURE_CLIENT_ID,
            secret=CONFIG.AZURE_SECRET,
            tenant=CONFIG.AZURE_TENANT
        )
        self._subscription_client = SubscriptionClient(self._credentials)
        self._compute_client = ComputeManagementClient(self._credentials, self._subscription_id)
        self._resource_client = ResourceManagementClient(self._credentials, self._subscription_id)
        self._network_client = NetworkManagementClient(self._credentials, self._subscription_id)
        self._monitor_client = MonitorClient(self._credentials, self._subscription_id)

        self._resource_groups = []
        for resource_group in self._resource_client.resource_groups.list():
            self._resource_groups.append(resource_group.name)

        for location in self._subscription_client.subscriptions.list_locations(self._subscription_id):
            self._regions.append(location.name)

        self._check_region()

    def list(self, disable_border=False, disable_header=False, state=None, notify=False, stop=False,
             warning_threshold=None, critical_threshold=None):
        if not state:
            state = ['running', 'stopped', 'starting', 'stopping', 'busy', 'generalized']
        table = prettytable.PrettyTable(['Region', 'RG', 'Name', 'Type', 'Image', 'State',
                                         'Launch time', 'Uptime', 'User', 'Private IP', 'Public IP',
                                         'Exclude'],
                                        border=not disable_border, header=not disable_header, reversesort=True,
                                        sortby='Launch time')
        table.align = 'l'
        i = 0
        states_dict = {}
        uptime_dict = {}
        name_dict = {}
        stop_dict = {}
        info_dict = {}
        warning_dict = {}
        critical_dict = {}
        dept_dict = {}
        rg_dict = {}
        local_tz = tzlocal.get_localzone()
        now = local_tz.localize(datetime.datetime.now())

        for resource_group in self._resource_groups:

            instances = self._compute_client.virtual_machines.list(resource_group)

            for instance in instances:
                region = instance.location
                if region not in self._regions:
                    continue

                last_user = ''
                start = datetime.datetime.now().date() - datetime.timedelta(days=7)
                afilter = " and ".join([
                    "eventTimestamp ge '%s'" % start,
                    "eventChannels eq 'Admin, Operation'",
                    "resourceUri eq '%s'" % instance.id
                ])
                select = ",".join([
                    "caller",
                    "eventName",
                    "operationName",
                    "eventTimestamp"
                ])
                activity_logs = self._monitor_client.activity_logs.list(
                    filter=afilter,
                    select=select
                )

                for log in activity_logs:
                    if log.caller and ("virtualMachines/start/action" in log.operation_name.value or
                                       "virtualMachines/write" in log.operation_name.value):
                        last_user = log.caller.split('@', 1)[0]
                        break

                instance_data = self._compute_client.virtual_machines.get(resource_group, instance.name,
                                                                          expand='instanceView')
                try:
                    instance_state = str(instance_data.instance_view.statuses[1].display_status).split('VM ')[1]
                except IndexError:
                    instance_state = 'busy'
                if instance_state == 'deallocated':
                    instance_state = 'stopped'
                elif instance_state == 'deallocating':
                    instance_state = 'stopping'

                if instance_state not in state:
                    if len(state) > 1:
                        print('UNKNOWN INSTANCE STATE: %s\n' % instance_state)
                    continue

                instance_type = instance_data.hardware_profile.vm_size
                image_name = instance_data.storage_profile.image_reference.offer + ' ' + \
                    instance_data.storage_profile.image_reference.sku

                nic_group = instance_data.network_profile.network_interfaces[0].id.split('/')[4]
                nic_name = instance_data.network_profile.network_interfaces[0].id.split('/')[8]

                net_interface = self._network_client.network_interfaces.get(nic_group, nic_name)
                private_ip_address = net_interface.ip_configurations[0].private_ip_address

                try:
                    ip_group = net_interface.ip_configurations[0].public_ip_address.id.split('/')[4]
                    ip_name = net_interface.ip_configurations[0].public_ip_address.id.split('/')[8]
                    public_ip_address = self._network_client.public_ip_addresses.get(ip_group, ip_name).ip_address or ''
                except AttributeError:
                    public_ip_address = ''

                uptime = ''
                excluded = False
                launch_time = ''

                if instance.tags:
                    excluded = True if instance.tags.get('EXCLUDE') else False

                if instance_state == 'running':
                    try:
                        launch_time_src = instance_data.instance_view.disks[0].statuses[0].time.astimezone(local_tz)
                        launch_time = launch_time_src.strftime('%Y-%m-%d %H:%M:%S')
                    except AttributeError:
                        launch_time_src = ''
                        launch_time = ''

                    seconds = self._date_diff(now, launch_time_src)
                    uptime = self._get_uptime(seconds)

                    if seconds >= critical_threshold and not excluded:
                        if resource_group not in stop_dict:
                            stop_dict[resource_group] = []
                        stop_dict[resource_group].append(instance.name)

                    if last_user and notify and not excluded:
                        if last_user not in dept_dict:
                            dept_dict[last_user] = []
                        if resource_group not in dept_dict[last_user]:
                            dept_dict[last_user].append(resource_group)
                        rg_dict[instance.name] = resource_group
                        name_dict[instance.name] = instance.name
                        uptime_dict[instance.name] = uptime
                        if last_user not in info_dict:
                            info_dict[last_user] = {}
                        if region not in info_dict[last_user]:
                            info_dict[last_user][region] = []
                        info_dict[last_user][region].append(instance.name)

                        if seconds >= critical_threshold:
                            critical_dict[last_user] = True
                        elif seconds >= warning_threshold:
                            warning_dict[last_user] = True

                i += 1
                table.add_row([
                    instance.location,
                    resource_group,
                    instance.name,
                    instance_type,
                    image_name,
                    instance_state,
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
            self._send_alert(
                mail_type=mail_type,
                user=user,
                region_ids=region_ids,
                name_dict=name_dict,
                uptime_dict=uptime_dict,
                warning_threshold=warning_threshold,
                critical_threshold=critical_threshold,
                stop=stop,
                dept=dept_dict[user],
                rg_dict=rg_dict
            )

        if stop and len(stop_dict) > 0:
            for rg, vms in stop_dict.items():
                print('\nStopping instances in Resource Group %s (%s)... %s' % (
                    rg,
                    ','.join(vms),
                    'SUCCESS' if self._stop_instance(rg, vms) else 'FAIL'))

    def _create_tag(self, resource_group, instance, key, value):
        try:
            self._compute_client.virtual_machines.create_or_update(resource_group, instance.name, {
                'location': instance.location,
                'tags': {key: value}
            }).wait()
        except CloudError:
            return False
        else:
            return True

    def _delete_tag(self, resource_group, instance, key):
        try:
            self._compute_client.virtual_machines.create_or_update(resource_group, instance.name, {
                'location': instance.location,
                'tags': {key: ''}
            }).wait()
        except CloudError:
            return False
        else:
            return True

    def tag(self, instance_id, key, value='', delete=False):
        i = 0
        resource_group = self._profile_name.upper()
        instances = self._compute_client.virtual_machines.list(resource_group)

        for instance in instances:
            if instance.name in instance_id:
                i += 1
                if delete:
                    print('Instance ID %s found in region %s, deleting tag \'%s\': ' %
                          (instance.name, instance.location, key), end='')
                    response = self._delete_tag(resource_group, instance, key)
                else:
                    print('Instance ID %s found in region %s, creating tag \'%s\': ' %
                          (instance.name, instance.location, key), end='')
                    response = self._create_tag(resource_group, instance, key, value)
                if response:
                    print('OK')
                else:
                    print('FAIL')
                if len(instance_id) == i:
                    return

        if i == 0:
            print('Instance ID %s not found in any region' % (', '.join(instance_id)))

    def _stop_instance(self, rg, vms):
        for vm in vms:
            self._compute_client.virtual_machines.deallocate(rg, vm).wait()
        return True

    def sg(self, *args, **kwargs):
        print('Command not implemented yet', file=sys.stderr)
        exit(1)

    def public_buckets(self, *args, **kwargs):
        print('Command not implemented yet', file=sys.stderr)
        exit(1)
