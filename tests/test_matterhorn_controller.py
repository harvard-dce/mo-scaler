import unittest
import os
from mock import patch, Mock, MagicMock
from requests.exceptions import Timeout

from moscaler.matterhorn import MatterhornController
from moscaler.exceptions import MatterhornCommunicationException
from pyhorn import MHClient


class TestMatterhornController(unittest.TestCase):
    def setUp(self):
        pass

    def test_constructor_online(self):

        with patch(
            "moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient
        ) as mock_pyhorn:
            with patch.dict(
                os.environ, {"MATTERHORN_USER": "foo", "MATTERHORN_PASS": "bar"}
            ):
                controller = MatterhornController("mh.example.edu")
                mock_pyhorn.assert_called_once_with(
                    "http://mh.example.edu", user="foo", passwd="bar", timeout=30
                )
                self.assertTrue(controller.is_online())

    def test_constructor_connect_error(self):

        controller = MatterhornController("mh.example.edu")
        self.assertFalse(controller.is_online())
        self.assertRaisesRegexp(
            MatterhornCommunicationException,
            "Error connecting",
            controller.verify_connection,
        )

    def test_constructor_connect_timeout(self):

        with patch(
            "moscaler.matterhorn.pyhorn.MHClient.me",
            side_effect=Timeout("timeout test"),
        ):
            controller = MatterhornController("mh.example.edu")
            self.assertFalse(controller.is_online())
            self.assertRaisesRegexp(
                MatterhornCommunicationException,
                "timeout test",
                controller.verify_connection,
            )

    @patch("moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient)
    def test_queued_job_counts(self, mock_pyhorn):

        controller = MatterhornController("mh.example.edu")
        controller.client = MagicMock(spec_set=MHClient)
        controller.client.workflows.return_value = [
            Mock(
                operations=[
                    Mock(
                        id="foo",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="RUNNING"), Mock(status="QUEUED")]
                        ),
                    ),
                    Mock(
                        id="foo",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="RUNNING"), Mock(status="QUEUED")]
                        ),
                    ),
                ]
            ),
            Mock(
                operations=[
                    Mock(
                        id="foo",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="QUEUED"), Mock(status="QUEUED")]
                        ),
                    ),
                    Mock(
                        id="bar",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="QUEUED"), Mock(status="QUEUED")]
                        ),
                    ),
                    Mock(id="foo", state="INSTANTIATED"),
                ]
            ),
            Mock(
                operations=[
                    Mock(id="foo", state="INSTANTIATED"),
                    Mock(
                        id="foo",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="RUNNING"), Mock(status="RUNNING")]
                        ),
                    ),
                    Mock(
                        id="bar",
                        state="WAITING",
                        job=Mock(
                            children=[Mock(status="QUEUED"), Mock(status="QUEUED")]
                        ),
                    ),
                    Mock(
                        id="baz",
                        state="RUNNING",
                        job=Mock(
                            children=[Mock(status="RUNNING"), Mock(status="QUEUED")]
                        ),
                    ),
                ]
            ),
        ]
        self.assertEqual(controller.queued_job_count(), 9)
        self.assertEqual(controller.queued_job_count(operation_types=["foo"]), 4)
        self.assertEqual(controller.queued_job_count(operation_types=["bar"]), 4)
        self.assertEqual(controller.queued_job_count(operation_types=["foo", "bar"]), 8)
        self.assertEqual(
            controller.queued_job_count(operation_types=["foo", "bar", "baz"]), 9
        )

    @patch("moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient)
    def test_is_registered(self, mock_pyhorn):

        controller = MatterhornController("http://mh.example.edu")
        controller._hosts = [Mock(base_url="foo"), Mock(base_url="bar")]
        self.assertTrue(controller.is_registered(Mock(mh_host_url="bar")))
        self.assertFalse(controller.is_registered(Mock(mh_host_url="blerg")))

    @patch("moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient)
    def test_is_idle(self, mock_pyhorn):

        controller = MatterhornController("http://mh.example.edu")
        controller._stats.running_jobs.return_value = 0
        self.assertTrue(controller.is_idle(Mock(mh_host_url="foo")))
        controller._stats.running_jobs.return_value = 1
        self.assertFalse(controller.is_idle(Mock(mh_host_url="foo")))

    @patch("moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient)
    def test_get_host(self, mock_pyhorn):

        controller = MatterhornController("http://mh.example.edu")
        controller._hosts = [Mock(id=1, base_url="foo"), Mock(id=2, base_url="bar")]
        self.assertEqual(controller.get_host(Mock(mh_host_url="foo")).id, 1)
        self.assertEqual(controller.get_host(Mock(mh_host_url="bar")).id, 2)

    @patch("moscaler.matterhorn.pyhorn.MHClient", spec_set=MHClient)
    def test_is_in_maintenance(self, mock_pyhorn):
        controller = MatterhornController("http://mh.example.edu")
        controller._hosts = [
            Mock(base_url="foo", maintenance=False),
            Mock(base_url="bar", maintenance=True),
        ]
        self.assertFalse(controller.is_in_maintenance(Mock(mh_host_url="foo")))
        self.assertTrue(controller.is_in_maintenance(Mock(mh_host_url="bar")))
