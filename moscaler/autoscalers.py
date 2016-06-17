
import os
import time
import boto3
import logging
from stat import ST_MTIME
from datetime import datetime, timedelta

LOGGER = logging.getLogger(__name__)

def create_autoscaler(type, controller):
    try:
        cls = globals()[type]
        return cls(controller)
    except KeyError:
        raise RuntimeError("Autoscaler class %s does not exist" % type)


class Autoscaler(object):

    def __init__(self, controller, pause_file_dir=None):
        self.controller = controller

        if pause_file_dir is None:
            pause_file_dir = os.path.expanduser('~')

        self.pause_file = os.path.join(pause_file_dir, '.moscaler-pause')

        try:
            self.num_up = int(os.environ.get('AUTOSCALE_UP_INCREMENT', 1))
            self.num_down = int(os.environ.get('AUTOSCALE_DOWN_INCREMENT', 1))
            self.scale_pause_interval = int(os.environ.get('AUTOSCALE_PAUSE_INTERVAL', 0))
        except Exception, e:
            raise RuntimeError("Missing or invalid ENV variable: %s", e)

    def pause_scaling(self):
        if not self.scale_pause_interval:
            LOGGER.debug("Autoscale pause disabled")
            return
        LOGGER.debug("Updating state file %s", self.pause_file)
        with open(self.pause_file, 'a'):
            os.utime(self.pause_file, None)

    def scaling_paused(self):
        if not self.scale_pause_interval:
            LOGGER.debug("Autoscale pause disabled")
            return False
        if not os.path.exists(self.pause_file):
            LOGGER.debug("State file %s does not exist", self.pause_file)
            return False
        pause_file_age = time.time() - os.stat(self.pause_file)[ST_MTIME]
        LOGGER.debug("State file %s is %d seconds old", self.pause_file, pause_file_age)
        if pause_file_age < self.scale_pause_interval:
            LOGGER.info("Scaling paused")
            return True
        else:
            LOGGER.info("Scaling ok")
            os.remove(self.pause_file)
            return False


class HighLoadJobs(Autoscaler):

    def __init__(self, *args, **kwargs):
        super(HighLoadJobs, self).__init__(*args, **kwargs)
        try:
            self.up_threshold = int(os.environ['AUTOSCALE_UP_THRESHOLD'])
        except Exception, e:
            raise RuntimeError("Missing or invalid ENV variable: %s", e)

    def scale(self):

        queued_jobs = self.controller.mhorn.queued_high_load_job_count()
        LOGGER.info("MH reports %d queued high load jobs", queued_jobs)

        if queued_jobs >= self.up_threshold and not self.scaling_paused():
            LOGGER.info("Attempting to scale up")
            self.controller._scale_up(self.num_up, scale_available=True)
            self.pause_scaling()
        else:
            with self.controller.mhorn.in_maintenance(
                    self.controller.online_workers,
                    dry_run=self.controller.dry_run):
                idle_workers = self.controller.idle_workers
                LOGGER.info("Idle workers: %d", len(idle_workers))
                if len(idle_workers):
                    LOGGER.info("Attempting to scale down")
                    self.controller._scale_down(
                        self.num_down,
                        check_uptime=True,
                        scale_available=True
                    )


class LayerLoad(Autoscaler):

    def __init__(self, *args, **kwargs):
        super(LayerLoad, self).__init__(*args, **kwargs)
        try:
            self.up_threshold = float(os.environ['AUTOSCALE_UP_THRESHOLD'])
            self.down_threshold = float(os.environ['AUTOSCALE_DOWN_THRESHOLD'])
            self.metric = os.environ['AUTOSCALE_LAYERLOAD_METRIC']
            self.layer_id = os.environ['AUTOSCALE_LAYERLOAD_LAYER_ID']
            self.sample_count = int(os.environ['AUTOSCALE_LAYERLOAD_SAMPLE_COUNT'])
            self.sample_period = int(os.environ['AUTOSCALE_LAYERLOAD_SAMPLE_PERIOD'])
        except Exception, e:
            raise RuntimeError("Missing or invalid ENV variable: %s", e)

    def _over_threshold(self, datapoints):
        return all(x >= self.up_threshold for x in datapoints)

    def _under_threshold(self, datapoints):
        return all(x < self.down_threshold for x in datapoints)

    def scale(self):

        cw = boto3.client('cloudwatch')

        LOGGER.debug("Fetching %d datapoints for metric %s", self.sample_count, self.metric)
        LOGGER.debug("Using layer id: %s", self.layer_id)

        start_time_seconds = self.sample_count * self.sample_period

        resp = cw.get_metric_statistics(
            Namespace="AWS/OpsWorks",
            MetricName=self.metric,
            Dimensions=[{
                'Name': 'LayerId',
                'Value': self.layer_id
            }],
            StartTime=datetime.utcnow() - timedelta(seconds=start_time_seconds),
            EndTime=datetime.utcnow(),
            Period=self.sample_period,
            Statistics=['Average']
        )

        datapoints = [x['Average'] for x in resp['Datapoints']]
        LOGGER.debug("Datapoints for %s: %s", self.metric, datapoints)

        # check if we're consistently over/under our thresholds for all datapoints
        if self._over_threshold(datapoints) and not self.scaling_paused():
            LOGGER.info("Attempting to scale up %d workers", self.num_up)
            self.controller._scale_up(self.num_up, scale_available=True)
            self.pause_scaling()
        elif self._under_threshold(datapoints):
            LOGGER.info("Attempting to scale down %d workers", self.num_down)
            with self.controller.mhorn.in_maintenance(
                    self.controller.online_workers,
                    dry_run=self.controller.dry_run):
                self.controller._scale_down(
                    self.num_down,
                    check_uptime=True,
                    scale_available=True
                )
        else:
            LOGGER.debug("No action to take.")


class LayerLoadPlusOnlineWorkers(LayerLoad):

    def _over_threshold(self, datapoints):
        adjusted_threshold = self.up_threshold + len(self.controller.online_workers)
        LOGGER.debug("Adjusted scale up threshold: %f", adjusted_threshold)
        return all(x >= adjusted_threshold for x in datapoints)

