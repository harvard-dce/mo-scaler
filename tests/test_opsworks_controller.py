
import unittest
from mock import patch, MagicMock
from datetime import datetime
from freezegun import freeze_time

import boto3
from moscaler.exceptions import *

boto3.setup_default_session(region_name='us-east-1')

from moscaler.opsworks import OpsworksController, OpsworksInstance

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
            'Instances': [{
                'InstanceId': '1',
                'Hostname': 'admin1',
                'PublicDns': 'http://mh.example.edu'
            }]
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

    def _create_instance(self, inst_dict, wrap=False):
        instance = OpsworksInstance(inst_dict, self.controller)
        if wrap:
            return MagicMock(wraps=instance)
        return instance

    def _create_instances(self, *inst_dicts, **kwargs):
        return [self._create_instance(x, **kwargs) for x in inst_dicts]

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

    def test_pending_workers(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': 1, 'Hostname': 'storage1', 'Status': 'online'},
            {'InstanceId': 2, 'Hostname': 'workers1', 'Status': 'booting'},
            {'InstanceId': 3, 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': 4, 'Hostname': 'workers3', 'Status': 'stopping'},
            {'InstanceId': 5, 'Hostname': 'admin1', 'Status': 'online'},
            {'InstanceId': 6, 'Hostname': 'engage1', 'Status': 'pending'},
            {'InstanceId': 7, 'Hostname': 'workers4', 'Status': 'pending'},
        )
        self.assertEqual(
            [2,7],
            [x.InstanceId for x in self.controller.pending_workers]
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

    def test_scale_down_not_enough_workers(self):

        self.controller._instances = self._create_instances(
            {'Hostname': 'workers1', 'Status': 'online'},
            {'Hostname': 'workers2', 'Status': 'online'},
            {'Hostname': 'workers3', 'Status': 'running_setup'},
        )

        self.assertRaisesRegexp(
            OpsworksScalingException,
            "does not have 4 online or pending",
            self.controller.scale_down, 4
        )

    @patch('moscaler.opsworks.MIN_WORKERS', 3)
    def test_scale_down_min_workers(self):

        self.controller._instances = self._create_instances(
            {'Hostname': 'workers1', 'Status': 'online'},
            {'Hostname': 'workers2', 'Status': 'online'},
            {'Hostname': 'workers3', 'Status': 'online'},
            {'Hostname': 'workers4', 'Status': 'online'},
        )

        self.assertRaisesRegexp(
            OpsworksScalingException,
            "Stopping 2 workers",
            self.controller.scale_down, 2
        )

    def test_scale_down(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': '1', 'Hostname': 'workers1', 'Status': 'online'},
            {'InstanceId': '2', 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': '3', 'Hostname': 'workers3', 'Status': 'online'},
            {'InstanceId': '4', 'Hostname': 'workers4', 'Status': 'online'},
            wrap=True
        )
        self.controller.mhorn.filter_idle.return_value = self.controller._instances
        self.controller.scale_down(2, check_uptime=False)
        self.assertEqual(
            [1,1,0,0],
            [x.stop.call_count for x in self.controller._instances]
        )

    def test_scale_down_uptime_check(self):

        instances = self._create_instances(
            {'InstanceId': '1', 'Hostname': 'workers1', 'Status': 'online'},
            {'InstanceId': '2', 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': '3', 'Hostname': 'workers3', 'Status': 'online'},
            {'InstanceId': '4', 'Hostname': 'workers4', 'Status': 'online'},
            wrap=True
        )

        instances[0]._mock_wraps.ec2_inst = MagicMock(launch_time=datetime(2015, 11, 13, 10, 45, 0))
        instances[1]._mock_wraps.ec2_inst = MagicMock(launch_time=datetime(2015, 11, 13, 10, 20, 0))
        instances[2]._mock_wraps.ec2_inst = MagicMock(launch_time=datetime(2015, 11, 13, 10, 04, 0))
        instances[3]._mock_wraps.ec2_inst = None

        self.controller._instances = instances
        self.controller.mhorn.filter_idle.return_value = self.controller._instances

        with freeze_time("2015-11-13 11:00:00"):
            self.controller.scale_down(2, check_uptime=True)
        self.assertEqual(
            [0,0,1,0],
            [x.stop.call_count for x in self.controller._instances]
        )

        for mock_inst in instances:
            mock_inst.reset_mock()

        with patch('moscaler.opsworks.IDLE_UPTIME_THRESHOLD', 30):
            with freeze_time("2015-11-13 12:00:00"):
                self.controller.scale_down(2, check_uptime=True)
        self.assertEqual(
            [0,1,1,0],
            [x.stop.call_count for x in self.controller._instances]
        )

        with patch('moscaler.opsworks.IDLE_UPTIME_THRESHOLD', 59):
            with freeze_time("2015-11-13 13:00:00"):
                self.assertRaisesRegexp(
                    OpsworksScalingException,
                    "No workers available to stop",
                    self.controller.scale_down,
                    2,
                    check_uptime=True
                )


    def test_scale_up(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': '1', 'Hostname': 'workers1', 'Status': 'online'},
            {'InstanceId': '2', 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': '3', 'Hostname': 'workers3', 'Status': 'stopped'},
            {'InstanceId': '4', 'Hostname': 'workers4', 'Status': 'online'},
            {'InstanceId': '5', 'Hostname': 'workers5', 'Status': 'stopped'},
            wrap=True
        )
        self.controller.scale_up(2)
        self.assertEqual(
            [0,0,1,0,1],
            [x.start.call_count for x in self.controller._instances]
        )

    def test_scale_up_not_enough_workers(self):

        self.controller._instances = self._create_instances(
            {'InstanceId': '1', 'Hostname': 'workers1', 'Status': 'online'},
            {'InstanceId': '2', 'Hostname': 'workers2', 'Status': 'online'},
            {'InstanceId': '3', 'Hostname': 'workers3', 'Status': 'stopped'},
            {'InstanceId': '4', 'Hostname': 'workers4', 'Status': 'online'},
            {'InstanceId': '5', 'Hostname': 'workers5', 'Status': 'stopped'},
        )
        self.assertRaisesRegexp(
            OpsworksScalingException,
            "Cluster does not have 3",
            self.controller.scale_up,
            3
        )
