# WANdisco Cloud module
#
# Version 16.5.16
#
# Author: Peter Pakos <peter.pakos@wandisco.com>

from __future__ import print_function
import abc
import boto3
import botocore.exceptions
import prettytable
import datetime
import pytz
import tzlocal
import wdmailer


class Cloud(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, cloud_provider, profile_name, region):
        self._cloud_provider = cloud_provider
        self._profile_name = profile_name
        self._region = region
        self._mail = wdmailer.Mail()
        self._heads = {
            'dev': ['yuri.yudin@wandisco.com', 'rob.budas@wandisco.com'],
            'qa': ['aandrew.heawood@wandisco.com', 'rrob.budas@wandisco.com', 'vvirginia.wang@wandisco.com'],
            'sales': ['scott.rudenstein@wandisco.com', 'rob.budas@wandisco.com'],
            'support': ['mark.kelly@wandisco.com', 'rob.budas@wandisco.com']
        }
        if profile_name in self._heads:
            self._head = self._heads[profile_name]
        else:
            self._head = None

    @staticmethod
    def loader(cloud_provider, profile_name, region):
        classes = {'aws': AWS, 'azure': AZURE, 'gce': GCE}
        return classes[cloud_provider](cloud_provider, profile_name, region)

    @abc.abstractmethod
    def describe_instances(self):
        pass

    @abc.abstractmethod
    def describe_regions(self):
        pass


class AWS(Cloud):
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
        self._regions = []
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
        if self._region:
            if self._region not in self._regions:
                print('Region must be one of the following:\n- %s' %
                      '\n- '.join(self._regions))
                exit(1)
            else:
                self._regions = [self._region]

    @staticmethod
    def _get_tag(list_a, search_key):
        value = None
        if type(list_a) == list:
            for item in list_a:
                if item['Key'] == search_key:
                    value = item['Value']
                    break
        return value

    @staticmethod
    def _date_diff(date1, date2):
        diff = (date1-date2)
        diff = (diff.microseconds + (diff.seconds + diff.days * 24 * 3600) * 10**6) / 10**6
        return diff

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

    def _send_alert(self, mail_type, user, region_ids, uptime_dict, warning_threshold, stop_threshold):
        number = 0
        table = prettytable.PrettyTable(['Zone', 'Instance ID', 'Uptime'])
        table.align = 'l'
        for region, ids in region_ids.items():
            number += len(ids)
            for iid in ids:
                table.add_row([
                    region,
                    iid,
                    uptime_dict[iid]
                ])
        s = ''
        have = 'has'
        if number > 1:
            s = 's'
            have = 'have'
        sender = 'Infrastructure & IT <infra@wandisco.com>'
        recipient = user + '@wandisco.com'
        cc_recipient = None
        if mail_type == 'info':
            subject = 'AWS %s: EC2 running instances' % str(self._profile_name).upper()
            message = '''Hi %s,

This is just a friendly reminder that you have %s running EC2 instance%s in AWS %s account:

%s

Make sure any running instances are either STOPPED or TERMINATED before close of business.

If you wish to keep your instances running for longer than %s hours, please raise a ticket using \
<a href="http://helpdesk.wandisco.com">IT Helpdesk</a> so we can exclude them from reporting.

Please note:
<li>any unexcluded instances running for longer than %s hours will be reported to the respective head of department</li\
><li>any unexcluded instances running for longer than %s hours will be automatically STOPPED.</li>
For more information check our \
<a href="https://workspace.wandisco.com/display/IT/AWS+Best+Practices+at+WANdisco">AWS Best Practices</a>.

Thank you.
''' % (
                user.split('.')[0].capitalize(),
                number,
                s,
                str(self._profile_name).upper(),
                table,
                warning_threshold/3600,
                warning_threshold/3600,
                stop_threshold/3600
            )
        else:
            cc_recipient = self._head
            subject = '*WARNING* AWS %s: EC2 running instances' % str(self._profile_name).upper()
            message = '''Hi %s,

You currently have %s EC2 instance%s in AWS %s account that %s been running for more than %s hours:

%s

Please STOP or TERMINATE any instances that are no longer in use.

If you wish to keep your instances running for more than %s hours, please raise a ticket using \
<a href="http://helpdesk.wandisco.com">IT Helpdesk</a> so we can exclude them from reporting.

For more information check our \
<a href="https://workspace.wandisco.com/display/IT/AWS+Best+Practices+at+WANdisco">AWS Best Practices</a>.

Thank you.
''' % (
                user.split('.')[0].capitalize(),
                number,
                s,
                str(self._profile_name).upper(),
                have,
                warning_threshold/3600,
                table,
                warning_threshold/3600
            )

        message += '\n-- \nInfrastructure & IT Team'
        cc = ''
        if cc_recipient:
            if type(cc_recipient) is list:
                cc = ' (cc: %s)' % ', '.join(cc_recipient)
            else:
                cc = ' (cc: %s)' % cc_recipient
        print('Sending %s email to %s%s... ' % (mail_type, recipient, cc), end='')
        status, msg = self._mail.send(sender, recipient, subject, message, html=True, cc=cc_recipient)
        print(msg['message'])

    def describe_instances(self, disable_border=False, disable_header=False, state=None, notify=False, stop=False,
                           warning_threshold=None, stop_threshold=None):
        if not state:
            state = ['running', 'pending', 'shutting-down', 'stopped', 'stopping', 'terminated']
        table = prettytable.PrettyTable(['Zone', 'ID', 'Name', 'Type', 'Image', 'State',
                                         'Launch time', 'Uptime', 'Last user', 'SSH key', 'Private IP', 'Public IP',
                                         'Exclude'],
                                        border=not disable_border, header=not disable_header, reversesort=True,
                                        sortby='Launch time')
        table.align = 'l'
        i = 0
        states_dict = {}
        notify_dict = {}
        uptime_dict = {}
        stop_dict = {}
        to_be_stopped_dict = {}
        local_tz = tzlocal.get_localzone()
        now = local_tz.localize(datetime.datetime.now())
        warning_dict = {}
        for region in self._regions:
            ec2r = self._session.resource('ec2', region_name=region)
            instances = ec2r.instances.filter(Filters=[{
                'Name': 'instance-state-name',
                'Values': state
            }])
            for instance in instances:
                i += 1
                excluded = True if self._get_tag(instance.tags, 'EXCLUDE') is not None else False
                image_name = ''
                private_ip_address = instance.private_ip_address if instance.private_ip_address is not None else ''
                public_ip_address = instance.public_ip_address if instance.public_ip_address is not None else ''
                instance_state = instance.state['Name']
                last_user = self._get_tag(instance.tags, 'Last_user') or ''
                uptime = ''
                name = self._get_tag(instance.tags, 'Name')
                if name is None:
                    name = ''
                then = instance.launch_time.astimezone(pytz.timezone(str(local_tz)))
                launch_time = str(then).partition('+')[0]
                if instance_state == 'running':
                    seconds = self._date_diff(now, then)
                    uptime = self._get_uptime(seconds)
                    if seconds >= stop_threshold and not excluded:
                        if last_user in to_be_stopped_dict:
                            if region in to_be_stopped_dict[last_user]:
                                to_be_stopped_dict[last_user][region].append(instance.id)
                            else:
                                to_be_stopped_dict[last_user][region] = []
                                to_be_stopped_dict[last_user][region].append(instance.id)
                        else:
                            to_be_stopped_dict[last_user] = {}
                            to_be_stopped_dict[last_user][region] = []
                            to_be_stopped_dict[last_user][region].append(instance.id)
                    if last_user and notify and not excluded:
                        uptime_dict[instance.id] = uptime
                        if last_user in notify_dict:
                            if region in notify_dict[last_user]:
                                notify_dict[last_user][region].append(instance.id)
                            else:
                                notify_dict[last_user][region] = []
                                notify_dict[last_user][region].append(instance.id)
                        else:
                            notify_dict[last_user] = {}
                            notify_dict[last_user][region] = []
                            notify_dict[last_user][region].append(instance.id)
                        if seconds >= warning_threshold:
                            warning_dict[last_user] = True
                        elif seconds >= stop_threshold:
                            stop_dict[last_user] = True
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
        if len(notify_dict) > 0:
            print()
        for user, region_ids in notify_dict.items():
            if user in to_be_stopped_dict:
                mail_type = 'alert'
            elif user in warning_dict:
                mail_type = 'warning'
            else:
                mail_type = 'info'
            self._send_alert(mail_type,
                             user,
                             region_ids,
                             uptime_dict,
                             warning_threshold=warning_threshold,
                             stop_threshold=stop_threshold)

    def describe_regions(self, disable_border=False, disable_header=False):
        table = prettytable.PrettyTable(['Region'], border=not disable_border, header=not disable_header,
                                        sortby='Region')
        table.align = 'l'
        for region in self._regions:
            table.add_row([region])
        print(table)

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


class AZURE(Cloud):
    def __init__(self, *args, **kwargs):
        super(AZURE, self).__init__(*args, **kwargs)
        print('%s module not implemented yet, exiting...' % self._cloud_provider.upper())
        exit(1)

    def describe_instances(self):
        pass

    def describe_regions(self):
        pass


class GCE(Cloud):
    def __init__(self, *args, **kwargs):
        super(GCE, self).__init__(*args, **kwargs)
        print('%s module not implemented yet, exiting...' % self._cloud_provider.upper())
        exit(1)

    def describe_instances(self):
        pass

    def describe_regions(self):
        pass
