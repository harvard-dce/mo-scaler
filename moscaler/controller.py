import boto3
from exceptions import OpsworksControllerException


class OpsworksController(object):

    def __init__(self, cluster, aws_profile=None):

        if aws_profile is not None:
            boto3.setup_default_session(profile_name=aws_profile)

        self.opsworks = boto3.client('opsworks')

        stacks = self.opsworks.describe_stacks()['Stacks']
        try:
            self.stack = next(x for x in stacks if x['Name'] == cluster)
        except StopIteration:
            raise OpsworksControllerException(
                "No opsworks stack named '%s' found" % cluster
            )

        instances = self.opsworks.describe_instances(
            StackId=self.stack['StackId']
        )['Instances']

        self.instances = [OpsworksInstance(x) for x in instances]

    def status(self):
        pass


class OpsworksInstance(object):

    def __init__(self, inst_dict):
        self._inst = inst_dict

    def __getattr__(self, k):
        try:
            return self._inst[k]
        except KeyError, e:
            raise AttributeError(k)

    def is_autoscale(self):
        return hasattr(self, 'AutoScalingType')

    def is_admin(self):
        return self.Hostname.startswith('admin')
