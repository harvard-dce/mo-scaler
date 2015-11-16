
import unittest
from datetime import datetime
from mock import patch, MagicMock, PropertyMock
from freezegun import freeze_time

from moscaler.opsworks import OpsworksInstance, OpsworksController


class TestOpsworksInstance(unittest.TestCase):

    def setUp(self):
        self.mock_controller = MagicMock(spec=OpsworksController)
        self.mock_controller.ec2 = MagicMock()

    def _create(self, inst_dict):
        return OpsworksInstance(inst_dict, self.mock_controller)

    def test_constructor(self):

        inst = self._create({'foo': 1})
        self.assertEqual(inst.foo, 1)
        self.assertIsNone(inst.ec2_inst)

    def test_repr(self):

        inst = self._create({'InstanceId': 'foo', 'Hostname': 'bar'})
        self.assertEqual(
            repr(inst),
            "<class 'moscaler.opsworks.OpsworksInstance'> (bar, foo, None)"
        )

    def test_repr_ec2_inst(self):

        inst = self._create({
            'InstanceId': 'foo',
            'Hostname': 'bar',
            'Ec2InstanceId': 'baz'})
        self.assertEqual(
            repr(inst),
            "<class 'moscaler.opsworks.OpsworksInstance'> (bar, foo, baz)"
        )

    def test_is_autoscale(self):

        self.assertTrue(self._create({'AutoScalingType': 'foo'}).is_autoscale())
        self.assertFalse(self._create({}).is_autoscale())

    def test_is_admin(self):

        self.assertTrue(self._create({'Hostname': 'admin'}).is_admin())
        self.assertTrue(self._create({'Hostname': 'admin99'}).is_admin())
        self.assertFalse(self._create({'Hostname': '99admin'}).is_admin())
        self.assertFalse(self._create({'Hostname': 'worker'}).is_admin())

    def test_is_worker(self):

        self.assertTrue(self._create({'Hostname': 'worker'}).is_worker())
        self.assertTrue(self._create({'Hostname': 'worker1'}).is_worker())
        self.assertFalse(self._create({'Hostname': '99worker'}).is_worker())
        self.assertFalse(self._create({'Hostname': 'admin'}).is_worker())

    def test_is_online(self):

        self.assertTrue(self._create({'Status': 'online'}).is_online())
        self.assertFalse(self._create({'Status': 'stopped'}).is_online())
        self.assertFalse(self._create({'Status': 'foo'}).is_online())

    def test_uptime(self):

        # no ec2 instance == 0 uptime
        inst = self._create({})
        self.assertEqual(inst.uptime(), 0)

        inst = self._create({})
        inst.ec2_inst = MagicMock(launch_time=datetime(2015, 11, 12, 12, 0, 0))
        with freeze_time('2015-11-12 15:32:09'):
            self.assertEqual(inst.uptime(), 12729)
        with freeze_time('2015-11-12 15:32:39'):
            self.assertEqual(inst.uptime(), 12759)

    def test_billed_minutes(self):

        inst = self._create({})
        self.assertEqual(inst.billed_minutes(), 0)

        inst = self._create({})
        inst.ec2_inst = MagicMock(launch_time=datetime(2015, 11, 12, 12, 0, 0))
        with freeze_time('2015-11-12 15:32:09'):
            self.assertEqual(inst.billed_minutes(), 32)


    def test_start(self):
        inst = self._create({})
        inst.start()
        self.mock_controller.start_instance.assert_called_once_with(inst)
        self.assertEqual(inst.action_taken, 'started')

    def test_stop(self):
        inst = self._create({})
        inst.stop()
        self.mock_controller.stop_instance.assert_called_once_with(inst)
        self.assertEqual(inst.action_taken, 'stopped')
