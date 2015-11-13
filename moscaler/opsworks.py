import arrow
import boto3
import logging
from os import getenv as env
from operator import methodcaller
from exceptions import *
from matterhorn import MatterhornController

log = logging.getLogger(__name__)

MIN_WORKERS = env('MOSCALER_MIN_WORKERS', 1)
IDLE_UPTIME_THRESHOLD = env('MOSCALER_IDLE_UPTIME_THRESHOLD', 50)


class OpsworksController(object):

    def __init__(self, cluster, force=False):

        self.force = force

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

        self.mh = MatterhornController(mh_admin['PublicDns'])
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
            "matterhorn_online": self.mh.is_online(),
            "instances": len(self.instances),
            "instances_online": len(self.online_instances),
            "workers": len(self.workers),
            "workers_online": len(self.online_workers),
            "workers_pending": len(self.pending_workers),
            "instances": []
        }

        status['job_status'] = self.mh.job_status()

        for inst in self.instances:
            inst_status = {
                "state": inst.Status,
                "opsworks_id": inst.InstanceId,
                "hostname": inst.Hostname
            }
            if hasattr(inst, 'Ec2InstanceId'):
                inst_status['ec2_id'] = inst.Ec2InstanceId
            status['instances'].append(inst_status)
        return status

    def scale_to(self, num_workers):

        current_workers = len(self.online_or_pending_workers)

        if current_workers == num_workers:
            raise OpsworksControllerException(
                "Cluster already at %d online or pending workers" \
                % num_workers
            )
        elif current_workers > num_workers:
            with self.mh.in_maintenance(self.online_workers):
                self.scale_down(current_workers - num_workers)
        else:
            self.scale_up(num_workers - current_workers)

    def scale_up(self, num_workers):
        pass

    def scale_auto(self):
        raise NotImplementedError()

    def scale_down(self, num_workers,
                   check_uptime=False, stop_candidates=None):

        current_workers = len(self.online_or_pending_workers)
        # do we have that many running workers?
        if current_workers - num_workers < 0:
            raise OpsworksScalingException(
                "Cluster does not have %d online or pending workers to stop" \
                % num_workers
            )

        if current_workers - num_workers < MIN_WORKERS:
            error_msg = "Stopping %d workers violates MIN_WORKERS setting of %d" \
                % (num_workers, MIN_WORKERS)
            if self.force:
                log.warning(error_msg)
            else:
                raise OpsworksScalingException(error_msg)

        if stop_candidates is None:
            stop_candidates = self.pending_workers
            stop_candidates += self.mh.filter_idle(self.online_workers)

        if len(stop_candidates) < num_workers:
            error_msg = "Cluster does not have %d idle or pending workers" \
                        % num_workers
            if self.force:
                log.warning(error_msg)
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

        if not check_uptime:
            instances_to_stop = stop_candidates[:num_workers]
        else:
            # only stop idle workers if they're approaching an uptime near to being
            # divisible by 60m since we're paying for the full hour anyway
            instances_to_stop = []
            for inst in stop_candidates:
                minutes = inst.billed_minutes()
                log.debug("Instance %s has used %d minutes of it's billing hour",
                          inst.InstanceId, minutes)
                if minutes < IDLE_UPTIME_THRESHOLD:
                    if self.force:
                        log.warning("Stopping %s anyway because --force", inst.InstanceId)
                    else:
                        log.debug("Not stopping %s", inst.InstanceId)
                        continue
                instances_to_stop.append(inst)
                if len(instances_to_stop) == num_workers:
                    break

            if not len(instances_to_stop):
                raise OpsworksScalingException("No workers available to stop")

        for inst in instances_to_stop:
            inst.stop()


class OpsworksInstance(object):

    def __init__(self, inst_dict, controller):
        self._inst = inst_dict
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
        except KeyError, e:
            raise AttributeError(k)

    @property
    def Ec2InstanceId(self):
        if 'Ec2InstanceId' in self._inst:
            return self._inst['Ec2InstanceId']
        return None

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

    def is_offline(self):
        return not self.online() and not self.is_pending()

    def in_maintenance(self):
        return self.controller.mh.maintenance_state(self.PrivateDns)

    def set_maintenance(self):
        self.controller.mh.set_maintenance_state(self.PrivateDns)

    def start(self):
        self.controller.opsworks.start_instance(InstanceId=self.InstanceId)

    def stop(self):
        self.controller.opsworks.stop_instance(InstanceId=self.InstanceId)
