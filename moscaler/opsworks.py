import json

import arrow
import boto3
import logging
from os import getenv as env
from operator import methodcaller, attrgetter
from moscaler.matterhorn import MatterhornController
from moscaler.exceptions import \
    OpsworksControllerException, \
    OpsworksScalingException

LOGGER = logging.getLogger(__name__)

MIN_WORKERS = env('MOSCALER_MIN_WORKERS', 1)
IDLE_UPTIME_THRESHOLD = env('MOSCALER_IDLE_UPTIME_THRESHOLD', 50)


class OpsworksController(object):

    def __init__(self, cluster, force=False, dry_run=False):

        self.force = force
        self.dry_run = dry_run

        self.opsworks = boto3.client('opsworks')
        self.ec2 = boto3.resource('ec2')
        stacks = self.opsworks.describe_stacks()['Stacks']
        try:
            self.stack = next(x for x in stacks if x['Name'] == cluster)
        except StopIteration:
            raise OpsworksControllerException(
                "No opsworks stack named '%s' found" % cluster
            )

        instances = self.opsworks.describe_instances(
            StackId=self.stack['StackId'])['Instances']

        try:
            mh_admin = next(x for x in instances
                            if x['Hostname'].startswith('admin'))
        except StopIteration:
            raise OpsworksControllerException("No admin node found")

        self.mhorn = MatterhornController(mh_admin['PublicDns'])
        self._instances = [OpsworksInstance(x, self) for x in instances]

    def __repr__(self):
        return "%s (%s)" % (self.__class__, self.stack['Name'])

    @property
    def instances(self):
        return [x for x in self._instances if not x.is_autoscale()]

    @property
    def workers(self):
        return [x for x in self.instances if x.is_worker()]

    @property
    def online_workers(self):
        return [x for x in self.workers if x.is_online()]

    @property
    def pending_workers(self):
        return [x for x in self.workers if x.is_pending()]

    @property
    def online_or_pending_workers(self):
        return self.online_workers + self.pending_workers

    @property
    def stopped_workers(self):
        return [x for x in self.workers if x.is_stopped()]

    @property
    def online_instances(self):
        return [x for x in self.instances if x.is_online()]

    @property
    def admin(self):
        try:
            return next(x for x in self.instances if x.is_admin())
        except StopIteration:
            raise OpsworksControllerException("No admin node found")

    def status(self):
        status = {
            "cluster": self.stack['Name'],
            "matterhorn_online": self.mhorn.is_online(),
            "instances": len(self.instances),
            "instances_online": len(self.online_instances),
            "workers": len(self.workers),
            "workers_online": len(self.online_workers),
            "workers_pending": len(self.pending_workers),
            "instance_details": []
        }

        status['job_status'] = self.mhorn.job_status()

        for inst in self.instances:
            inst_status = {
                "state": inst.Status,
                "opsworks_id": inst.InstanceId,
                "hostname": inst.Hostname
            }
            if inst in self.workers:
                inst_status.update(self.mhorn.node_status(inst))
            if hasattr(inst, 'Ec2InstanceId'):
                inst_status['ec2_id'] = inst.Ec2InstanceId
            status['instance_details'].append(inst_status)
        LOGGER.debug("Cluster status: %s", json.dumps(status))
        return status

    def action_summary(self):
        stopped = [x for x in self.workers
                   if x.action_taken == 'stopped']
        started = [x for x in self.workers
                   if x.action_taken == 'started']
        return {
            'total_stopped': len(stopped),
            'stopped': '; '.join("%r" % x for x in stopped),
            'total_started': len(started),
            'started': ', '.join("%s" % x for x in started)
        }

    def start_instance(self, inst):
        LOGGER.info("Starting %r", inst)
        if not self.dry_run:
            self.opsworks.start_instance(InstanceId=inst.InstanceId)

    def stop_instance(self, inst):
        LOGGER.info("Stopping %r", inst)
        if not self.dry_run:
            self.opsworks.stop_instance(InstanceId=inst.InstanceId)

    def scale_to(self, num_workers):

        current_workers = len(self.online_or_pending_workers)

        if current_workers == num_workers:
            raise OpsworksControllerException(
                "Cluster already at %d online or pending workers!"
                % num_workers
            )
        elif current_workers > num_workers:
            self.scale('down', current_workers - num_workers)
        else:
            self.scale('up', num_workers - current_workers)

    def scale(self, direction, num_workers=None):

        if direction == "up":
            LOGGER.info("Attempting to scale up %d workers", num_workers)
            self._scale_up(num_workers)
        else:
            with self.mhorn.in_maintenance(self.online_workers,
                                           dry_run=self.dry_run):
                if direction == "down":
                    LOGGER.info("Attempting to scale down %d workers",
                                num_workers)
                    self._scale_down(num_workers)
                elif direction == "auto":
                    LOGGER.info("Initiating auto-scaling")
                    self._scale_auto()

    def _scale_auto(self):
        raise NotImplementedError()

    def _scale_up(self, num_workers):

        # do we have enough non-running workers?
        if len(self.stopped_workers) < num_workers:
            raise OpsworksScalingException(
                "Cluster does not have {} to start!".format(num_workers))

        # prefer instances that already have an associated ec2 instance
        start_candidates = sorted(
            self.stopped_workers,
            key=attrgetter('ec2_inst'),
            reverse=True
        )

        instances_to_start = start_candidates[:num_workers]
        LOGGER.info("Starting %d workers", len(instances_to_start))
        for inst in instances_to_start:
            inst.start()

    def _scale_down(self, num_workers,
                    check_uptime=False, stop_candidates=None):

        current_workers = len(self.online_or_pending_workers)

        # do we have that many running workers?
        if current_workers - num_workers < 0:
            raise OpsworksScalingException(
                "Cluster does not have %d online or pending workers to stop!"
                % num_workers
            )

        if current_workers - num_workers < MIN_WORKERS:
            error_msg = "Stopping %d workers violates MIN_WORKERS %d!" \
                % (num_workers, MIN_WORKERS)
            if self.force:
                LOGGER.warning(error_msg)
            else:
                raise OpsworksScalingException(error_msg)

        if stop_candidates is None:
            stop_candidates = self.pending_workers
            stop_candidates += [x for x in self.online_workers
                                if self.mhorn.is_idle(x)]

        if len(stop_candidates) < num_workers:
            error_msg = "Cluster does not have %d idle or pending workers!" \
                        % num_workers
            if self.force:
                LOGGER.warning(error_msg)
                # just pick from running workers
                stop_candidates = self.online_or_pending_workers
            else:
                raise OpsworksScalingException(error_msg)

        # helps ensure we're stopping the longest-running, and also that we'll
        # stop the same instance again if (for some reason) an earlier stop
        # action got wedged
        stop_candidates = sorted(
            stop_candidates,
            key=methodcaller('uptime'),
            reverse=True)

        if check_uptime:
            stop_candidates = self.filter_by_billing_hour(stop_candidates)

        instances_to_stop = stop_candidates[:num_workers]

        if not len(instances_to_stop):
            raise OpsworksScalingException("No workers available to stop!")

        LOGGER.info("Stopping %d workers", len(instances_to_stop))
        for inst in instances_to_stop:
            inst.stop()

    def filter_by_billing_hour(self, instances, uptime_threshold=None):
        """
        only stop idle workers if approaching uptime near to being
        divisible by 60m since we're paying for the full hour anyway
        """

        if uptime_threshold is None:
            uptime_threshold = IDLE_UPTIME_THRESHOLD

        filtered_instances = []
        for inst in instances:
            minutes = inst.billed_minutes()
            LOGGER.debug(
                "Instance %s has used %d minutes of it's billing hour",
                inst.InstanceId,
                minutes
            )
            if minutes < uptime_threshold:
                if self.force:
                    LOGGER.warning("Including %r because --force",
                                   inst)
                else:
                    LOGGER.debug("Not including %r", inst)
                    continue
            filtered_instances.append(inst)
        return filtered_instances


class OpsworksInstance(object):

    def __init__(self, inst_dict, controller):
        self._inst = inst_dict
        self.action_taken = None
        self.controller = controller
        self.ec2_inst = None
        if 'Ec2InstanceId' in inst_dict:
            self.ec2_inst = controller.ec2.Instance(inst_dict['Ec2InstanceId'])

    def __repr__(self):
        return "%s (%s, %s, %s)" % (
            self.__class__,
            self.Hostname,
            self.InstanceId,
            self.Ec2InstanceId
        )

    def __getattr__(self, k):
        try:
            return self._inst[k]
        except KeyError:
            raise AttributeError(k)

    @property
    def Ec2InstanceId(self):
        if 'Ec2InstanceId' in self._inst:
            return self._inst['Ec2InstanceId']
        return None

    @property
    def mh_host_url(self):
        return 'http://' + self.PrivateDns

    def uptime(self):
        if self.ec2_inst is None:
            return 0
        launch_time = arrow.get(self.ec2_inst.launch_time)
        now = arrow.utcnow()
        return (now - launch_time).seconds

    def billed_minutes(self):
        return (self.uptime() / 60) % 60

    def is_autoscale(self):
        return hasattr(self, 'AutoScalingType')

    def is_admin(self):
        return self.Hostname.startswith('admin')

    def is_worker(self):
        return self.Hostname.startswith('worker')

    def is_online(self):
        return self.Status == 'online'

    def is_pending(self):
        return self.Status in [
            'pending',
            'requested',
            'running_setup',
            'booting',
            'rebooting'
        ]

    def is_stopped(self):
        return self.Status == "stopped"

    def start(self):
        self.controller.start_instance(self)
        self.action_taken = 'started'

    def stop(self):
        self.controller.stop_instance(self)
        self.action_taken = 'stopped'
