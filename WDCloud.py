# WANdisco Cloud module
#
# Version 17.3.31
#
# Author: Peter Pakos <peter.pakos@wandisco.com>

from __future__ import print_function
import os
import abc
import datetime
import boto3
import botocore.exceptions
import prettytable
import tzlocal
from WDMail import WDMail
from CONFIG import CONFIG
from oauth2client.client import GoogleCredentials, HttpAccessTokenRefreshError
from googleapiclient import discovery
import iso8601
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.subscriptions import SubscriptionClient


class WDCloud(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, cloud_provider, profile_name, region):
        self._cloud_provider = cloud_provider
        self._profile_name = profile_name
        self._region = region
        self._regions = []
        self._mail = WDMail()
        self._heads = CONFIG.HEADS
        if profile_name in self._heads:
            self._head = self._heads[profile_name]
        else:
            self._head = None
        self._compute_engine = {
            'AWS': 'EC2',
            'GCP': 'GCE',
            'AZURE': 'AZURE'
        }
        self._bp_url = {
            'AWS': 'https://workspace.wandisco.com/display/IT/AWS+Best+Practices+at+WANdisco',
            'GCP': 'https://workspace.wandisco.com/display/IT/GCP+Best+Practices+at+WANdisco',
            'AZURE': 'https://workspace.wandisco.com/display/IT/AZURE+Best+Practices+at+WANdisco'
        }

    @staticmethod
    def loader(cloud_provider, profile_name, region):
        classes = {'aws': AWS, 'azure': AZURE, 'gcp': GCP}
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
        d = divmod(seconds, 86400)
        h = divmod(d[1], 3600)
        m = divmod(h[1], 60)
        s = m[1]
        uptime = []
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

    def _send_alert(self, mail_type, user, region_ids, name_dict, uptime_dict, warning_threshold, alert_threshold,
                    stop=False):
        profile = self._profile_name.upper()
        user_name = user.split('.')[0].capitalize()
        cloud = self._cloud_provider.upper()
        engine = self._compute_engine[cloud]
        bp_url = self._bp_url[cloud]
        number = 0
        table = prettytable.PrettyTable(['Region', 'Instance ID', 'Name', 'Uptime'])
        table.align = 'l'
        for region, ids in region_ids.items():
            number += len(ids)
            for iid in ids:
                table.add_row([
                    region,
                    iid,
                    name_dict[iid],
                    uptime_dict[iid]
                ])
        s = ''
        have = 'has'
        if number > 1:
            s = 's'
            have = 'have'
        sender = CONFIG.EMAIL_FROM
        recipient = user + '@' + CONFIG.EMAIL_DOMAIN
        cc_recipient = None
        subject = None
        message = None
        if mail_type == 'info':
            subject = '%s %s: %s running instances' % (cloud, profile, engine)
            message = '''Hi %s,

This is just a friendly reminder that you have %s running %s instance%s in %s %s account:

%s

Make sure any running instances are either STOPPED or TERMINATED before close of business.

If you wish to keep your instances running for longer than %s hours, please raise a ticket using \
<a href="https://helpdesk.wandisco.com">IT Helpdesk</a> so we can exclude them from reporting.

Please note:
<li>any unexcluded instances running for longer than %s hours will be reported to the respective head of department</li\
><li>any unexcluded instances running for longer than %s hours will be automatically STOPPED.</li>
For more information check our <a href="%s">%s Best Practices</a>.

Thank you.
''' % (
                user_name,
                number,
                engine,
                s,
                cloud,
                profile,
                table,
                warning_threshold / 3600,
                warning_threshold / 3600,
                alert_threshold / 3600,
                bp_url,
                cloud
            )
        elif mail_type == 'warning':
            subject = '*WARNING* %s %s: %s running instances' % (cloud, profile, engine)
            cc_recipient = self._head
            if recipient in cc_recipient:
                cc_recipient.remove(recipient)
            if number > 1:
                some_of_them = 'and either all or some of them'
            else:
                some_of_them = 'that'
            message = '''Hi %s,

You currently have %s %s instance%s in %s %s account %s %s been running for longer than %s hours:

%s

<strong>Please STOP or TERMINATE any instances that are no longer in use.</strong>

If you wish to keep your instances running for longer than %s hours, please raise a ticket using \
<a href="https://helpdesk.wandisco.com">IT Helpdesk</a> so we can exclude them from reporting.

For more information check our <a href="%s">%s Best Practices</a>.

Thank you.
''' % (
                user_name,
                number,
                engine,
                s,
                cloud,
                profile,
                some_of_them,
                have,
                warning_threshold / 3600,
                table,
                warning_threshold / 3600,
                bp_url,
                cloud
            )
        elif mail_type == 'critical':
            subject = '*CRITICAL* %s %s: %s running instances' % (cloud, profile, engine)
            cc_recipient = self._head
            if recipient in cc_recipient:
                cc_recipient.remove(recipient)
            if stop:
                stop_msg = '\n<strong>ANY INSTANCES RUNNING FOR LONGER THAN %s HOURS WILL BE STOPPED IMMEDIATELY!\
</strong>\n\nPlease check your %s account and make sure there are no more offending instances.\n' % \
                           ((alert_threshold / 3600), cloud)
            else:
                stop_msg = '\n<strong>PLEASE IMMEDIATELY STOP OR TERMINATE ANY INSTANCES THAT ARE NO LONGER IN USE!\
    </strong>\n'
            message = '''Hi %s,

You currently have %s %s instance%s in %s %s account that %s been running for longer than %s hours:

%s
%s
As per <a href="%s">%s Best Practices</a>, if \
you wish to keep your instances running for more than %s hours, please raise a ticket using \
<a href="https://helpdesk.wandisco.com">IT Helpdesk</a> so we can exclude them from reporting.

Thank you.
''' % (
                user_name,
                number,
                engine,
                s,
                cloud,
                profile,
                have,
                alert_threshold / 3600,
                table,
                stop_msg,
                bp_url,
                cloud,
                warning_threshold / 3600
            )
        else:
            print('We should not reach this part!')
            exit(1)

        message += '\n-- \nInfrastructure & IT Team'
        cc = ''
        if cc_recipient:
            if type(cc_recipient) is list:
                cc = ' (cc: %s)' % ', '.join(cc_recipient)
            else:
                cc = ' (cc: %s)' % cc_recipient
        print('Sending %s email to %s%s... ' % (mail_type, recipient, cc), end='')
        response = self._mail.send(sender, recipient, subject, message, html=True, cc=cc_recipient)
        if response == 202:
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
             warning_threshold=None, alert_threshold=None):
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

                    if seconds >= alert_threshold and not excluded:
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

                        if seconds >= alert_threshold:
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
                             alert_threshold,
                             stop)
        if stop and len(stop_dict) > 0:
            for region, iids in stop_dict.items():
                print('\nStopping instances in region %s (%s)... %s' % (
                    region,
                    ','.join(iids),
                    'success' if self._stop_instance(region, iids) else 'fail')
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
             warning_threshold=None, alert_threshold=None):
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
                launch_time_src = self._operations_get(operations, instance_id, 'endTime')
                launch_time_src = iso8601.parse_date(launch_time_src).astimezone(local_tz)
                launch_time = launch_time_src.strftime('%Y-%m-%d %H:%M:%S')\
                    if launch_time_src else ''
                uptime = ''
                excluded = False

                if instance_state == 'running':
                    seconds = self._date_diff(now, launch_time_src)
                    uptime = self._get_uptime(seconds)
                    if seconds >= alert_threshold and not excluded:
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

                        if seconds >= alert_threshold:
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
                             alert_threshold,
                             stop)


class AZURE(WDCloud):
    def __init__(self, *args, **kwargs):
        super(AZURE, self).__init__(*args, **kwargs)
        subscription_id = CONFIG.AZURE_SUBSCRIPTION_ID
        credentials = ServicePrincipalCredentials(
            client_id=CONFIG.AZURE_CLIENT_ID,
            secret=CONFIG.AZURE_SECRET,
            tenant=CONFIG.AZURE_TENANT
        )
        client = SubscriptionClient(credentials)
        for location in client.subscriptions.list_locations(subscription_id):
            self._regions.append(location.name)

        self._check_region()

    def list(self):
        pass
