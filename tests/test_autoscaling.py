
import os
import time
import shutil
import unittest
import tempfile
from mock import patch, MagicMock, PropertyMock
from datetime import datetime
from freezegun import freeze_time

from moscaler.opsworks import OpsworksController
from moscaler.autoscalers import Autoscaler, create_autoscaler


class TestAutoscalingBase(unittest.TestCase):

    def setUp(self):
        self.mock_controller = MagicMock(spec=OpsworksController)
        self.pause_file_dir = tempfile.mkdtemp()

        mock_mhorn = PropertyMock(return_value=MagicMock())
        type(self.mock_controller).mhorn = mock_mhorn

    def tearDown(self):
        shutil.rmtree(self.pause_file_dir)

    def _create(self, type=None):
        if type is None:
            return Autoscaler(self.mock_controller, self.pause_file_dir)
        else:
            return create_autoscaler(type, self.mock_controller)


class TestAutoscaler(TestAutoscalingBase):

    def test_scaling_paused_no_interval(self):
        self.assertFalse(self._create().scaling_paused())

    @patch.dict(os.environ, {'AUTOSCALE_PAUSE_INTERVAL': '100'})
    def test_scaling_paused_no_pause_file(self):
        self.assertFalse(self._create().scaling_paused())

    @patch.dict(os.environ, {'AUTOSCALE_PAUSE_INTERVAL': '100'})
    def test_pause_scaling(self):
        autoscaler = self._create()
        self.assertFalse(autoscaler.scaling_paused())
        autoscaler.pause_scaling()
        self.assertTrue(autoscaler.scaling_paused())

    @patch.dict(os.environ, {'AUTOSCALE_PAUSE_INTERVAL': '100'})
    def test_pause_scaling_expire(self):
        autoscaler = self._create()
        self.assertFalse(autoscaler.scaling_paused())
        autoscaler.pause_scaling()
        self.assertTrue(autoscaler.scaling_paused())
        three_minutes_ago = time.time() - 180
        os.utime(autoscaler.pause_file, (three_minutes_ago, three_minutes_ago))
        self.assertFalse(autoscaler.scaling_paused())


LAYER_LOAD_TEST_ENV = {
    'AUTOSCALE_UP_THRESHOLD': '1.0',
    'AUTOSCALE_DOWN_THRESHOLD': '2.0',
    'AUTOSCALE_LAYERLOAD_METRIC': 'foo-metric',
    'AUTOSCALE_LAYERLOAD_LAYER_ID': 'abc123',
    'AUTOSCALE_LAYERLOAD_SAMPLE_COUNT': '5',
    'AUTOSCALE_LAYERLOAD_SAMPLE_PERIOD': '60'
}

class TestLayerLoadAutoscaling(TestAutoscalingBase):

    @patch.dict(os.environ, LAYER_LOAD_TEST_ENV)
    def test_over_under_threshold(self):
        autoscaler = self._create(type='LayerLoad')
        self.assertTrue(autoscaler._over_threshold([1.1, 2.0, 100.0, 57.3, 9]))
        self.assertFalse(autoscaler._over_threshold([1, 1.0, 1.7, 0.5, 0, 3.3]))
        self.assertTrue(autoscaler._under_threshold([1.9, 0.5, 0, 1, 1.0]))
        self.assertFalse(autoscaler._under_threshold([2.0, 2, 3.1, 10, 1, 0.5]))

    @patch.dict(os.environ, LAYER_LOAD_TEST_ENV)
    def test_autoscale_up(self):

        autoscaler = self._create(type='LayerLoad')
        autoscaler.scaling_paused = MagicMock(return_value=False)
        autoscaler.pause_scaling = MagicMock()
        mock_cloudwatch = MagicMock()
        mock_cloudwatch.get_metric_statistics.return_value = {
            'Datapoints': [{'Average': x} for x in [2.0, 2.5, 10.5]]
        }
        with patch('boto3.client', return_value=mock_cloudwatch):
            autoscaler.scale()

        self.assertEquals(self.mock_controller._scale_up.call_count, 1)
        self.assertEquals(autoscaler.pause_scaling.call_count, 1)

    @patch.dict(os.environ, LAYER_LOAD_TEST_ENV)
    def test_autoscale_up_scaling_paused(self):

        autoscaler = self._create(type='LayerLoad')
        autoscaler.scaling_paused = MagicMock(return_value=True)
        autoscaler.pause_scaling = MagicMock()
        mock_cloudwatch = MagicMock()
        mock_cloudwatch.get_metric_statistics.return_value = {
            'Datapoints': [{'Average': x} for x in [2.0, 2.5, 10.5]]
        }
        with patch('boto3.client', return_value=mock_cloudwatch):
            autoscaler.scale()

        self.assertEquals(self.mock_controller._scale_up.call_count, 0)
        self.assertEquals(autoscaler.pause_scaling.call_count, 0)


    @patch.dict(os.environ, LAYER_LOAD_TEST_ENV)
    def test_autoscale_down(self):

        autoscaler = self._create(type='LayerLoad')
        mock_cloudwatch = MagicMock()
        mock_cloudwatch.get_metric_statistics.return_value = {
            'Datapoints': [{'Average': x} for x in [1.0, 1.5, 0.5]]
        }
        with patch('boto3.client', return_value=mock_cloudwatch):
            self.mock_controller.dry_run = False
            autoscaler.scale()

        self.assertEquals(self.mock_controller._scale_down.call_count, 1)

