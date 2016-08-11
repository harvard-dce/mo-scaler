
import os
import time
import shutil
import unittest
import tempfile
from mock import patch, MagicMock, PropertyMock
from datetime import datetime
from freezegun import freeze_time

from moscaler.opsworks import OpsworksController
from moscaler.autoscale import Autoscaler


class TestAutoscaling(unittest.TestCase):

#    def setUp(self):
#        self.pause_file_dir = tempfile.mkdtemp()
#
#    def tearDown(self):
#        shutil.rmtree(self.pause_file_dir)

    def _create(self, config=None):
        pause_file_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, pause_file_dir)
        mock_controller = MagicMock(spec=OpsworksController)
        mock_mhorn = PropertyMock(return_value=MagicMock())
        type(mock_controller).mhorn = mock_mhorn
        type(mock_controller).dry_run = False
        return Autoscaler(mock_controller, config, pause_file_dir)

    def test_scaling_paused_no_interval(self):
        self.assertFalse(self._create().scaling_paused())

    def test_scaling_paused_no_pause_file(self):
        self.assertFalse(self._create().scaling_paused())

    def test_scaling_paused_empty_pause_file(self):
        autoscaler = self._create()
        open(autoscaler.pause_file, 'a').close()
        self.assertFalse(autoscaler.scaling_paused())

    def test_pause_scaling(self):
        autoscaler = self._create()
        self.assertFalse(autoscaler.scaling_paused())
        autoscaler.pause_scaling(1)
        self.assertTrue(autoscaler.scaling_paused())
        # check again to confirm
        self.assertTrue(autoscaler.scaling_paused())
        autoscaler._tick_pause_cycles()
        self.assertFalse(autoscaler.scaling_paused())
        autoscaler._write_pause_file(-1)
        self.assertFalse(autoscaler.scaling_paused())

    def test_tick_pause_cycles(self):
        autoscaler = self._create()
        self.assertFalse(autoscaler.scaling_paused())
        self.assertEqual(autoscaler._read_pause_file(), 0)
        autoscaler.pause_scaling(5)
        self.assertEqual(autoscaler._read_pause_file(), 5)
        autoscaler._tick_pause_cycles()
        self.assertEqual(autoscaler._read_pause_file(), 4)
        autoscaler._tick_pause_cycles()
        autoscaler._tick_pause_cycles()
        autoscaler._tick_pause_cycles()
        self.assertEqual(autoscaler._read_pause_file(), 1)
        autoscaler._tick_pause_cycles()
        self.assertEqual(autoscaler._read_pause_file(), 0)
        self.assertFalse(os.path.exists(autoscaler.pause_file))

    def test_up_or_down(self):
        autoscaler = self._create()

        def _check(direction, up_thresh, down_thresh, dps):
            self.assertEqual(
                direction,
                autoscaler._up_or_down(dps, up_thresh, down_thresh)
            )

        _check('up', 10.0, 4.0, [11, 100.0, 49.55, 20])
        _check('up', 2, 1, [2, 2.3, 99])
        _check(None, 2, 1, [2, 2.3, 1.6])
        _check(None, 20.0, 10.0, [1.0, 30.0, 15])
        _check(None, 10, 5, [1, 3.3, 5])
        _check(None, 2, 1, [])
        _check('down', 10, 5, [1, 3.3, 4.9])
        _check('down', 2, 1, [0.2, 0.3, 0.9])

    def test_scale_up_or_down(self):

        config = {
                'pause_cycles': 1,
                'up_increment': 1,
                'down_increment': 1
        }
        checks = [
            ({'foo': 'up'}, '_scale_up'),
            ({'foo': 'up', 'bar': 'down'}, '_scale_up'),
            ({'foo': 'up', 'bar': None}, '_scale_up'),
            ({}, None),
            ({'foo': None, 'bar': None}, None),
            ({'foo': 'down', 'bar': None}, None),
            ({'foo': 'down', 'bar': 'down'}, '_scale_down')
        ]

        for results, method in checks:
            autoscaler = self._create(config=config)
            autoscaler._scale_up_or_down(results)
            if method is not None:
                self.assertEquals(getattr(autoscaler.controller, method).call_count, 1)
            else:
                self.assertEquals(autoscaler.controller._scale_up.call_count, 0)
                self.assertEquals(autoscaler.controller._scale_down.call_count, 0)

