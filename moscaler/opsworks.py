import boto3
from exceptions import OpsworksControllerException
from matterhorn import MatterhornController

class OpsworksController(object):

    def __init__(self, cluster, aws_profile=None):

        if aws_profile is not None:
            boto3.setup_default_session(profile_name=aws_profile)

        self.opsworks = boto3.client('opsworks')

        stacks = self.opsworks.describe_stacks()['Stacks']
        try:
            self.stack_id = next(x for x in stacks if x['Name'] == cluster)
        except StopIteration:
            raise OpsworksControllerException(
                "No opsworks stack named '%s' found" % cluster
            )

        instances = self.opsworks.describe_instances(
            StackId=self.stack_id)['Instances']

        try:
            mh_admin = next(x for x in instances
                            if x['Hostname'].startswith('admin'))
        except StopIteration:
            raise OpsworksControllerException("No admin node found")

        self.mh = MatterhornController(mh_admin['PublicDns'])
        self._instances = [OpsworksInstance(x, self.opsworks, self.mh)
                           for x in instances]

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
            "cluster": self._stack['Name'],
            "matterhorn_online": self.mh.is_online(),
            "instances": len(self.instances),
            "instances_online": len(self.online_instances),
            "workers": len(self.workers),
            "workers_online": len(self.online_workers),
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

    def scale_to(self, workers):

        running_workers = len(self.online_workers)

        if running_workers == workers:
            raise OpsworksControllerException(
                "Cluster already at %d running workers" % workers
            )
        elif running_workers > workers:
            self.scale_down(running_workers - workers)
        else:
            self.scale_up(workers - running_workers)

    def scale_up(self, workers):
        pass

    def scale_down(self, workers):
        pass

    def scale_auto(self):
        pass

class OpsworksInstance(object):

    def __init__(self, inst_dict, opsworks, mh):
        self._inst = inst_dict
        self.mh = mh
        self.opsworks = opsworks

    def __getattr__(self, k):
        try:
            return self._inst[k]
        except KeyError, e:
            raise AttributeError(k)

    def is_autoscale(self):
        return hasattr(self, 'AutoScalingType')

    def is_admin(self):
        return self.Hostname.startswith('admin')

    def is_worker(self):
        return self.Hostname.startswith('worker')

    def is_online(self):
        return self.Status == 'online'

    def start(self):
        self.opsworks.start_instance(InstanceId=self.InstanceId)

    def stop(self):
        self.opsworks.stop_instance(InstanceId=self.InstanceId)
