
import boto3
import unittest
from mock import patch, MagicMock

from moscaler import OpsworksController, OpsworksInstance

class TestOpsworksController(unittest.TestCase):

    def setUp(self):

        mock_opsworks = MagicMock(spec_set=boto3.client('opsworks'))
        mock_opsworks.describe_stacks.return_value = {
            'Stacks': [
                {'StackId': 'abcd1234', 'Name': 'test-stack'}
            ]
        }
        # must provide an 'admin' instance for mh controller creation
        mock_opsworks.describe_instances.return_value = {
            'Instances': [
                { 'Hostname': 'admin1', 'PublicDns': 'http://mh.example.edu' }
            ]
        }
        self.mock_boto3 = patch(
            'boto3.client',
            autospec=True,
            return_value=mock_opsworks
        )
        self.mock_mh = patch(
            'moscaler.opsworks.MatterhornController',
            autospec=True
        )
        self.mock_boto3.start()
        self.mock_mh.start()
        self.addCleanup(self.mock_boto3.stop)
        self.addCleanup(self.mock_mh.stop)

        self.controller = OpsworksController('test-stack')

    def _create_instance(self, inst_dict):
        return OpsworksInstance(inst_dict, self.controller, self.controller.mh)

    def test_instances(self):

        self.controller._instances = [
            self._create_instance({'InstanceId': 1}),
            self._create_instance({'InstanceId': 2, 'AutoScalingType': 'foo'}),
            self._create_instance({'InstanceId': 3})
        ]
        self.assertEqual([1,3], [x.InstanceId for x in self.controller.instances])


