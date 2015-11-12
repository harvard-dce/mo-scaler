
import unittest
from mock import patch, MagicMock

import boto3
from moscaler.exceptions import OpsworksControllerException

boto3.setup_default_session(region_name='us-east-1')

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
        self.mock_mh = patch('moscaler.opsworks.MatterhornController')

        self.mock_boto3.start()
        self.mock_mh.start()
        self.addCleanup(self.mock_boto3.stop)
        self.addCleanup(self.mock_mh.stop)

        self.controller = OpsworksController('test-stack')

    def _create_instance(self, inst_dict):
        return OpsworksInstance(inst_dict, self.controller)

    def _create_instances(self, *inst_dicts):
        return [self._create_instance(x) for x in inst_dicts]

    def test_instances(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': 1},
            {'InstanceId': 2, 'AutoScalingType': 'foo'},
            {'InstanceId': 3}
        )
        self.assertEqual(
            [1,3],
            [x.InstanceId for x in self.controller.instances]
        )

    def test_workers(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': 1, 'Hostname': 'workers1'},
            {'InstanceId': 2, 'Hostname': 'workers2'},
            {'InstanceId': 3, 'Hostname': 'engage1'},
            {'InstanceId': 4, 'Hostname': 'workers3'},
            {'InstanceId': 5, 'Hostname': 'admin1'},
        )
        self.assertEqual(
            [1,2,4],
            [x.InstanceId for x in self.controller.workers]
        )

    def test_online_workers(self):
        self.controller._instances = self._create_instances(
            {'InstanceId': 1, 'Hostname': 'storage1'},
            {'InstanceId': 2, 'Hostname': 'workers1', 'Status': 'stopped'},
            {'InstanceId': 3, 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': 4, 'Hostname': 'workers3', 'Status': 'online'},
            {'InstanceId': 5, 'Hostname': 'admin1'},
        )
        self.assertEqual(
            [3,4],
            [x.InstanceId for x in self.controller.online_workers]
        )

    def test_admin(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': 1, 'Hostname': 'workers1'},
            {'InstanceId': 2, 'Hostname': 'engage1'},
            {'InstanceId': 3, 'Hostname': 'admin1'}
        )
        self.assertEqual(self.controller.admin.InstanceId, 3)

    def test_no_admin(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': 1, 'Hostname': 'workers1'},
            {'InstanceId': 2, 'Hostname': 'engage1'},
            {'InstanceId': 3, 'Hostname': 'workers2'}
        )

        self.assertRaises(
            OpsworksControllerException,
            self.controller.__getattribute__,
            'admin'
        )

    def test_scale_to(self):

        self.controller._instances = self._create_instances(
            {'Hostname': 'workers1', 'Status': 'online'},
            {'Hostname': 'workers2', 'Status': 'online'},
            {'Hostname': 'workers3', 'Status': 'online'},
            {'Hostname': 'workers4', 'Status': 'online'}
        )

        with patch.object(self.controller, 'scale_up', autospec=True) as scale_up:
            self.controller.scale_to(9)
            scale_up.assert_called_once_with(5)

        with patch.object(self.controller, 'scale_down', autospec=True) as scale_down:
            self.controller.scale_to(2)
            scale_down.assert_called_once_with(2)

        self.assertRaises(
            OpsworksControllerException,
            self.controller.scale_to,
            4
        )




