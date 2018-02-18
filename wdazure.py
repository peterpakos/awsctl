# -*- coding: utf-8 -*-
"""This module provides Azure class.

Author: Peter Pakos <peter.pakos@wandisco.com>

Copyright (C) 2019 WANdisco
"""

from __future__ import print_function
import datetime
import prettytable
import tzlocal
import iso8601

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.subscriptions import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.monitor import MonitorClient
from msrestazure.azure_exceptions import CloudError

from CONFIG import CONFIG
import logging
from wdcloud import WDCloud

log = logging.getLogger('cloud_tools')


class AZURE(WDCloud):
    def __init__(self, *args, **kwargs):
        super(AZURE, self).__init__(*args, **kwargs)

        account = 'OLD' if 'old' in str(self._profile_name).lower() else ''

        self._subscription_id = getattr(CONFIG, account + 'AZURE_SUBSCRIPTION_ID')
        self._credentials = ServicePrincipalCredentials(
            client_id=getattr(CONFIG, account + 'AZURE_CLIENT_ID'),
            secret=getattr(CONFIG, account + 'AZURE_SECRET'),
            tenant=getattr(CONFIG, account + 'AZURE_TENANT')
        )
        self._subscription_client = SubscriptionClient(self._credentials)
        self._compute_client = ComputeManagementClient(self._credentials, self._subscription_id)
        self._resource_client = ResourceManagementClient(self._credentials, self._subscription_id)
        self._network_client = NetworkManagementClient(self._credentials, self._subscription_id)
        self._monitor_client = MonitorClient(self._credentials, self._subscription_id)
        self._hdi_client = HDInsightManagementClient(self._credentials, self._subscription_id)

        self._resource_groups = []
        for resource_group in self._resource_client.resource_groups.list():
            self._resource_groups.append(resource_group.name)

        for location in self._subscription_client.subscriptions.list_locations(self._subscription_id):
            self._regions.append(location.name)

    def list_hdi(self, warning_threshold, critical_threshold, disable_border, disable_header, notify, stop,
                 *args, **kwargs):
        table = prettytable.PrettyTable(['Location', 'Name', 'Resource Group', 'Creator', 'Created Date', 'Uptime',
                                         'Cluster State', 'Excluded'], sortby='Created Date',
                                        border=not disable_border, header=not disable_header, reversesort=True)
        table.align = 'l'
        local_tz = tzlocal.get_localzone()
        now = local_tz.localize(datetime.datetime.now())
        clusters = self._hdi_client.clusters.list()
        states_dict = {}
        stop_dict = {}
        dept_dict = {}
        rg_dict = {}
        name_dict = {}
        uptime_dict = {}
        info_dict = {}
        warning_dict = {}
        critical_dict = {}
        i = 0
        while True:
            try:
                for cluster in clusters.advance_page():
                    i += 1
                    rg = cluster.id
                    _, _, rg = rg.partition('/resourceGroups/')
                    rg, _, _ = rg.partition('/providers/')
                    created_date = iso8601.parse_date(cluster.properties.created_date).astimezone(local_tz).\
                        strftime('%Y-%m-%d %H:%M:%S')
                    launch_time_src = iso8601.parse_date(cluster.properties.created_date).astimezone(local_tz)
                    seconds = self._date_diff(now, launch_time_src)
                    uptime = self._get_uptime(seconds)

                    start = datetime.datetime.now().date() - datetime.timedelta(days=30)
                    afilter = " and ".join([
                        "eventTimestamp ge '%s'" % start,
                        "eventChannels eq 'Admin, Operation'",
                        "resourceUri eq '%s'" % cluster.id
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

                    creator = ''
                    for alog in activity_logs:
                        if alog.caller and ("Microsoft.HDInsight/clusters/write" in alog.operation_name.value):
                            creator = alog.caller.split('@', 1)[0]
                            break

                    if cluster.tags:
                        excluded = True if 'exclude' in [t.lower() for t in cluster.tags] else False
                    else:
                        excluded = False

                    table.add_row([cluster.location, cluster.name, rg, creator, created_date, uptime,
                                   cluster.properties.cluster_state, 'Yes' if excluded else 'No'])

                    if cluster.properties.cluster_state in states_dict:
                        states_dict[cluster.properties.cluster_state] += 1
                    else:
                        states_dict[cluster.properties.cluster_state] = 1

                    if seconds >= (critical_threshold * 3600) and not excluded and\
                            'sales' not in str(rg).lower() and cluster.properties.cluster_state != 'Deleting':
                        if rg not in stop_dict:
                            stop_dict[rg] = []
                        stop_dict[rg].append(cluster.name)

                    if creator and notify and not excluded and cluster.properties.cluster_state != 'Deleting':
                        if creator not in dept_dict:
                            dept_dict[creator] = []
                        if rg.partition('-')[0] not in dept_dict[creator]:
                            dept_dict[creator].append(rg.partition('-')[0])
                        rg_dict[cluster.name] = rg
                        name_dict[cluster.name] = cluster.name
                        uptime_dict[cluster.name] = uptime
                        if creator not in info_dict:
                            info_dict[creator] = {}
                        if cluster.location not in info_dict[creator]:
                            info_dict[creator][cluster.location] = []
                        info_dict[creator][cluster.location].append(cluster.name)

                        if seconds >= (critical_threshold * 3600):
                            critical_dict[creator] = True
                        elif seconds >= (warning_threshold * 3600):
                            warning_dict[creator] = True
            except StopIteration:
                break

        log.debug('info_dict: %s' % info_dict)
        log.debug('warning_dict: %s' % warning_dict)
        log.debug('critical_dict: %s' % critical_dict)
        log.info(table)
        out = ', '.join(['%s: %s' % (key, value) for (key, value) in sorted(states_dict.items())])
        if len(out) > 0:
            out = '(%s)' % out
        else:
            out = ''
        print('Time: %s (%s) | Clusters: %s %s' % (now.strftime('%Y-%m-%d %H:%M:%S'), str(local_tz), i, out))

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
                rg_dict=rg_dict,
                resource='HDI cluster'
            )

        if stop and len(stop_dict) > 0:
            for rg, vms in stop_dict.items():
                print('\nTerminating HDI clusters in Resource Group %s (%s)... %s' % (
                    rg,
                    ','.join(vms),
                    'SUCCESS' if self._delete_cluster(rg, vms) else 'FAIL'))

    def list(self, disable_border=False, disable_header=False, state=None, notify=False, stop=False,
             warning_threshold=None, critical_threshold=None, tag=None, *args, **kwargs):
        if not state:
            state = ['running', 'stopped', 'starting', 'stopping', 'busy', 'generalized']
        table = prettytable.PrettyTable(['Region', 'RG', 'Name', 'Type', 'Image', 'State',
                                         'Launch time', 'Uptime', 'User', 'Private IP', 'Public IP',
                                         'Excluded'],
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
                start = datetime.datetime.now().date() - datetime.timedelta(days=30)
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

                for alog in activity_logs:
                    if alog.caller and ("virtualMachines/start/action" in alog.operation_name.value or
                                        "virtualMachines/write" in alog.operation_name.value):
                        last_user = alog.caller.split('@', 1)[0]
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

                try:
                    image_name = instance_data.storage_profile.image_reference.offer + ' ' + \
                        instance_data.storage_profile.image_reference.sku
                except (AttributeError, TypeError):
                    image_name = ''

                nic_group = instance_data.network_profile.network_interfaces[0].id.split('/')[4]
                nic_name = instance_data.network_profile.network_interfaces[0].id.split('/')[8]

                try:
                    net_interface = self._network_client.network_interfaces.get(nic_group, nic_name)
                    private_ip_address = net_interface.ip_configurations[0].private_ip_address
                except CloudError:
                    private_ip_address = ''

                net_interface = None
                try:
                    ip_group = net_interface.ip_configurations[0].public_ip_address.id.split('/')[4]
                    ip_name = net_interface.ip_configurations[0].public_ip_address.id.split('/')[8]
                    public_ip_address = self._network_client.public_ip_addresses.get(ip_group, ip_name).ip_address or ''
                except AttributeError:
                    public_ip_address = ''

                uptime = ''
                launch_time = ''

                if instance.tags:
                    excluded = True if 'exclude' in [t.lower() for t in instance.tags] else False
                else:
                    excluded = False

                if instance_state == 'running':
                    try:
                        launch_time_src = instance_data.instance_view.disks[0].statuses[0].time.astimezone(local_tz)
                        launch_time = launch_time_src.strftime('%Y-%m-%d %H:%M:%S')
                    except AttributeError:
                        launch_time_src = ''
                        launch_time = ''

                    seconds = self._date_diff(now, launch_time_src)
                    uptime = self._get_uptime(seconds)

                    if seconds >= (critical_threshold * 3600) and not excluded and\
                            'sales' not in str(resource_group).lower():
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

                        if seconds >= (critical_threshold * 3600):
                            critical_dict[last_user] = True
                        elif seconds >= (warning_threshold * 3600):
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
                    'Yes' if excluded else 'No'
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

    def tag(self, instance_id, key, value='', delete=False, *args, **kwargs):
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
        error = False

        for vm in vms:
            try:
                self._compute_client.virtual_machines.deallocate(rg, vm).wait()
            except Exception as e:
                log.debug(e)
                error = True

        return False if error else True

    def _delete_cluster(self, rg, clusters):
        error = False

        for cluster in clusters:
            try:
                self._hdi_client.clusters.delete(rg, cluster)
            except Exception as e:
                log.debug(e)
                error = True

        return False if error else True

    def sg(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def public_buckets(self, *args, **kwargs):
        log.critical('Command not implemented')
        exit(1)

    def run(self, *args, **kwargs):
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
