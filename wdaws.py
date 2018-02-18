# -*- coding: utf-8 -*-
"""This module provides AWS class.

Author: Peter Pakos <peter.pakos@wandisco.com>

Copyright (C) 2019 WANdisco
"""

from __future__ import print_function
import os
import datetime
import boto3
import botocore.exceptions
import prettytable
import tzlocal
import wdcloud

import logging

log = logging.getLogger('cloud_tools')


class AWS(wdcloud.WDCloud):
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
             warning_threshold=None, critical_threshold=None, tag=None, *args, **kwargs):
        tag_key = None
        tag_value = None
        if tag:
            tag_key = tag.partition(':')[0]
            tag_value = tag.partition(':')[2]

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
            instances = ec2r.instances.filter(Filters=[
                {'Name': 'instance-state-name', 'Values': state},
            ])
            for instance in instances:
                if tag_key:
                    if tag_value:
                        if self._get_tag(instance.tags, tag_key) != tag_value:
                            continue
                    else:
                        if not self._get_tag(instance.tags, tag_key):
                            continue

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

                    if seconds >= (critical_threshold * 3600) and not excluded:
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

                        if seconds >= (critical_threshold * 3600):
                            critical_dict[last_user] = True
                        elif seconds >= (warning_threshold * 3600):
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

    def _start_instance(self, region, instance_ids):
        ec2r = self._session.resource('ec2', region_name=region)
        response = ec2r.instances.filter(InstanceIds=instance_ids).start()
        if response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def _terminate_instance(self, region, instance_ids):
        ec2r = self._session.resource('ec2', region_name=region)
        response = ec2r.instances.filter(InstanceIds=instance_ids).terminate()
        if response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def _create_tag(self, region, resource, key, value):
        ec2c = self._session.client('ec2', region_name=region)
        response = ec2c.create_tags(Resources=[resource], Tags=[{
            'Key': key,
            'Value': value
        }])
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def _delete_tag(self, region, resource, key):
        ec2c = self._session.client('ec2', region_name=region)
        ec2c.delete_tags(Resources=[resource], Tags=[{
            'Key': key
        }])
        return True

    def tag(self, instance_id, key, value='', delete=False, *args, **kwargs):
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
            print('Instance ID %s not found in any region' % ', '.join(instance_id))

    def sg(self, cidr, delete=False, *args, **kwargs):
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

    def public_buckets(self, disable_border=False, disable_header=False, *args, **kwargs):
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

    def _run(self, number, region, subnet_id, image_id, instance_type, ssh_key, private_ip=None, volume_size=10,
             user_data=''):
        ec2c = self._session.client('ec2', region_name=region)
        response = None

        if volume_size:
            bdm = [{'DeviceName': '/dev/sda1', 'Ebs': {'DeleteOnTermination': True, 'VolumeSize': volume_size}}]
        else:
            bdm = [{'DeviceName': '/dev/sda1', 'Ebs': {'DeleteOnTermination': True}}]

        try:
            if private_ip:
                response = ec2c.run_instances(
                    MinCount=1, MaxCount=number, SubnetId=subnet_id, ImageId=image_id, InstanceType=instance_type,
                    KeyName=ssh_key, PrivateIpAddress=private_ip, BlockDeviceMappings=bdm, UserData=user_data
                )
            else:
                response = ec2c.run_instances(
                    MinCount=1, MaxCount=number, SubnetId=subnet_id, ImageId=image_id, InstanceType=instance_type,
                    KeyName=ssh_key, BlockDeviceMappings=bdm, UserData=user_data
                )
        except botocore.exceptions.ClientError as e:
            log.critical(e)
            exit(1)

        instances = []
        for instance in response['Instances']:
            instances.append({
                'id': instance['InstanceId'],
                'private_ip': instance['PrivateIpAddress'],
                'private_dns_name': instance['PrivateDnsName'],
                'image_id': instance['ImageId']
            })

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return instances
        else:
            return False

    def _wait_for_instances(self, region, iid, state='running'):
        ec2r = self._session.resource('ec2', region_name=region)
        instance = ec2r.Instance(iid)
        if state == 'running':
            instance.wait_until_running()
        elif state == 'stopped':
            instance.wait_until_stopped()
        elif state == 'terminated':
            instance.wait_until_terminated()

    def _wait_for_images(self, region, iid, state='available'):
        ec2r = self._session.resource('ec2', region_name=region)
        image = ec2r.Image(iid)
        image.wait_until_exists(Filters=[{'Name': 'image-id', 'Values': [iid]},
                                         {'Name': 'state', 'Values': [state]}])
        return True

    @staticmethod
    def _wait_net_service(host, port, timeout=300):
        import socket
        import time
        end = time.time() + timeout

        while True:
            s = socket.socket()
            if time.time() > end:
                s.close()
                return False
            if s.connect_ex((host, port)) == 0:
                s.close()
                return True
            else:
                s.close()
            time.sleep(1)

    def _delete_on_termination(self, region, iid):
        ec2c = self._session.client('ec2', region_name=region)
        response = ec2c.describe_instance_attribute(
            InstanceId=iid,
            Attribute='blockDeviceMapping'
        )

        bdm = []
        for i in response['BlockDeviceMappings']:
            bdm.append({'DeviceName': i['DeviceName'], 'Ebs': {'DeleteOnTermination': True}})

        response = ec2c.modify_instance_attribute(
            InstanceId=iid,
            Attribute='blockDeviceMapping',
            BlockDeviceMappings=bdm
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def run(self, region, subnet_id, image_id_list, ssh_key, instance_type, count=1, private_ip=None,
            volume_size=None, tag=None, user_data='', name=None, *args, **kwargs):
        self._check_region(region)
        s = 's' if count > 1 else ''
        creator = os.getenv('BUILD_USER_ID', os.getenv('USER', 'unknown'))
        ssh_key = ssh_key if ssh_key else creator

        if len(image_id_list) != count and len(image_id_list) > 1:
            log.critical('Number of images needs to be equal to 1 or instance count (%s)' % count)
            exit(1)

        image_id = image_id_list[0] if len(image_id_list) == 1 else None

        log.info('Creating %s %s instance%s in region %s as user %s (SSH key %s)...' %
                 (count, instance_type, s, region, creator, ssh_key))
        instances = []
        for i in range(count):
            if len(image_id_list) > 1 and count > 1:
                image_id = image_id_list[i]
            ip = self._ip_sum(private_ip, i) if private_ip else None
            r = self._run(1, region, subnet_id, image_id, instance_type, ssh_key, ip, volume_size, user_data)
            if r:
                instances += r
            else:
                log.critical('Failed to create instance')
                exit(1)
        for i in instances:
            log.info('%s\t%s\t%s\t%s' % (i['id'], i['private_ip'], i['private_dns_name'], i['image_id']))

        if os.getenv('JOB_NAME') and 'DEMO-' in os.getenv('JOB_NAME'):
            demo_env = os.getenv('JOB_NAME').partition('_')[0]
            log.info('Adding %s environment tags...' % demo_env)
            for i in instances:
                if self._create_tag(region, i['id'], 'Demo', demo_env):
                    log.info('%s\tOK' % i['private_dns_name'])
                else:
                    log.info('%s\tFAIL' % i['private_dns_name'])
        else:
            demo_env = None

        log.info('Adding %sName tags...' % ('%s ' % demo_env if demo_env else ''))
        for i in instances:
            if self._create_tag(region, i['id'], 'Name', '%s%s' %
                                                         (('%s ' % demo_env) if demo_env else '',
                                                          str(i['private_dns_name']).partition('.')[0])):
                log.info('%s\tOK' % i['private_dns_name'])
            else:
                log.info('%s\tFAIL' % i['private_dns_name'])

        log.info('Adding Last_user tags...')
        for i in instances:
            if self._create_tag(region, i['id'], 'Last_user', creator):
                log.info('%s\tOK' % i['private_dns_name'])
            else:
                log.info('%s\tFAIL' % i['private_dns_name'])

        log.info('Waiting for instance%s to start...' % s)
        for i in instances:
            self._wait_for_instances(region, i['id'])
        log.info('%snstance%s %s running' % ('All i' if s else 'I', s, 'are' if s else 'is'))

        log.info('Waiting for SSH to come up...')
        for i in instances:
            if self._wait_net_service(i['private_dns_name'], 22):
                log.info('%s\tOK' % i['private_dns_name'])
            else:
                log.info('%s\tTIMEOUT' % i['private_dns_name'])

        return instances

    def create_image(self, region, instance_ids, tag,  *args, **kwargs):
        self._check_region(region)
        if not tag and not instance_ids:
            log.critical('Please specify instance(s) using either -i or -t')
            exit(1)
        instance_ids = [] if not instance_ids else instance_ids
        tag_key = None
        tag_value = None

        if tag:
            tag_key = tag.partition(':')[0]
            tag_value = tag.partition(':')[2]

        ec2r = self._session.resource('ec2', region_name=region)
        ec2c = self._session.client('ec2', region_name=region)
        instances = ec2r.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['stopped']},
        ])

        instances_to_image = []
        for instance in instances:
            if tag_key:
                if tag_value:
                    if self._get_tag(instance.tags, tag_key) == tag_value:
                        instances_to_image.append(instance)
                        continue
                else:
                    if self._get_tag(instance.tags, tag_key):
                        instances_to_image.append(instance)
                        continue
            if instance.id in instance_ids:
                instances_to_image.append(instance)

        n = len(instances_to_image)

        if not n:
            log.info('No stopped instances found%s' % (' with tag %s' % tag if tag else ''))
            return True

        s = 's' if n > 1 else ''
        log.info('Creating image%s of %s instance%s in region %s...' % (s, n, s, region))

        if os.getenv('JOB_NAME') and 'DEMO-' in os.getenv('JOB_NAME'):
            demo_env = os.getenv('JOB_NAME').partition('_')[0]
        else:
            demo_env = None

        build_number = os.getenv('BUILD_NUMBER')

        image_ids = []
        for instance in instances_to_image:
            r = None
            try:
                r = ec2c.create_image(InstanceId=instance.id, Name='%s%s%s' % (
                    ('%s ' % demo_env) if demo_env else '',
                    ('%s ' % build_number) if build_number else '',
                    str(instance.private_dns_name).partition('.')[0]
                ))
            except botocore.exceptions.ClientError as err:
                log.critical(err)
                exit(1)
            image_id = r.get('ImageId')
            image_ids.append(image_id)
            log.info('%s\t%s\t%s' % (instance.id, image_id, '%s%s%s' % (
                ('%s ' % demo_env) if demo_env else '',
                ('%s ' % build_number) if build_number else '',
                str(instance.private_dns_name).partition('.')[0]
            )))

        log.info('Waiting for AMIs to become available...')
        for i in image_ids:
            if self._wait_for_images(region, i):
                log.info('%s\tOK' % i)
            else:
                log.info('%s\tTIMEOUT' % i)

    def stop(self, region, instance_ids, tag, *args, **kwargs):
        self._check_region(region)
        instance_ids = [] if not instance_ids else instance_ids
        tag_key = None
        tag_value = None
        if tag:
            tag_key = tag.partition(':')[0]
            tag_value = tag.partition(':')[2]

        ec2r = self._session.resource('ec2', region_name=region)
        instances = ec2r.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running']},
        ])

        instances_to_stop = []
        for instance in instances:
            if tag_key:
                if tag_value:
                    if self._get_tag(instance.tags, tag_key) == tag_value:
                        instances_to_stop.append(instance.id)
                        continue
                else:
                    if self._get_tag(instance.tags, tag_key):
                        instances_to_stop.append(instance.id)
                        continue
            if instance.id in instance_ids:
                instances_to_stop.append(instance.id)

        n = len(instances_to_stop)

        if not n:
            log.critical('No running instances found%s' % (' with tag %s' % tag if tag else ''))
            exit(1)

        s = 's' if n > 1 else ''
        log.info('Stopping %s instance%s in region %s...' % (n, s, region))
        if not self._stop_instance(region, instances_to_stop):
            log.critical('Failed to stop instances')
            exit(1)

        log.info('Waiting for instance%s to stop...' % s)
        for i in instances_to_stop:
            self._wait_for_instances(region, i, 'stopped')
        log.info('%snstance%s %s stopped' % ('All i' if s else 'I', s, 'are' if s else 'is'))

    def start(self, region, instance_ids, tag, *args, **kwargs):
        self._check_region(region)
        instance_ids = [] if not instance_ids else instance_ids
        tag_key = None
        tag_value = None
        if tag:
            tag_key = tag.partition(':')[0]
            tag_value = tag.partition(':')[2]

        ec2r = self._session.resource('ec2', region_name=region)
        instances = ec2r.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['stopped']},
        ])

        instances_to_start = []
        for instance in instances:
            if tag_key:
                if tag_value:
                    if self._get_tag(instance.tags, tag_key) == tag_value:
                        instances_to_start.append(instance)
                        continue
                else:
                    if self._get_tag(instance.tags, tag_key):
                        instances_to_start.append(instance)
                        continue
            if instance.id in instance_ids:
                instances_to_start.append(instance)

        n = len(instances_to_start)

        if not n:
            log.critical('No stopped instances found%s' % (' with tag %s' % tag if tag else ''))
            exit(1)

        s = 's' if n > 1 else ''
        log.info('Starting %s instance%s in region %s...' % (n, s, region))
        if not self._start_instance(region, [instance.id for instance in instances_to_start]):
            log.critical('Failed to start instances')
            exit(1)

        log.info('Waiting for instance%s to start...' % s)
        for i in instances_to_start:
            self._wait_for_instances(region, i.id, 'running')
        log.info('%snstance%s %s started' % ('All i' if s else 'I', s, 'are' if s else 'is'))

        log.info('Waiting for SSH to come up...')
        for i in instances_to_start:
            if self._wait_net_service(i.private_dns_name, 22):
                log.info('%s\tOK' % i.private_dns_name)
            else:
                log.info('%s\tTIMEOUT' % i.private_dns_name)

    def terminate(self, region, instance_ids, tag, *args, **kwargs):
        if not tag and not instance_ids:
            log.critical('Please specify instance(s) using either -i or -t')
            exit(1)
        self._check_region(region)
        instance_ids = [] if not instance_ids else instance_ids
        tag_key = None
        tag_value = None
        if tag:
            tag_key = tag.partition(':')[0]
            tag_value = tag.partition(':')[2]

        ec2r = self._session.resource('ec2', region_name=region)
        instances = ec2r.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'stopped']},
        ])

        instances_to_terminate = []
        for instance in instances:
            if tag_key:
                if tag_value:
                    if self._get_tag(instance.tags, tag_key) == tag_value:
                        instances_to_terminate.append(instance.id)
                        continue
                else:
                    if self._get_tag(instance.tags, tag_key):
                        instances_to_terminate.append(instance.id)
                        continue
            if instance.id in instance_ids:
                instances_to_terminate.append(instance.id)

        n = len(instances_to_terminate)

        if not n:
            log.info('No running/stopped instances found')
            return True

        s = 's' if n > 1 else ''
        log.info('Terminating %s instance%s in region %s...' % (n, s, region))
        if not self._terminate_instance(region, instances_to_terminate):
            log.critical('Failed to terminate instances')
            exit(1)

        log.info('Waiting for instance%s to terminate...' % s)
        for i in instances_to_terminate:
            self._wait_for_instances(region, i, 'terminated')
        log.info('%snstance%s %s terminated' % ('All i' if s else 'I', s, 'are' if s else 'is'))

    def list_hdi(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)
