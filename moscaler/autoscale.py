
import os
import boto3
import logging
from datetime import datetime, timedelta
from operator import itemgetter
from moscaler.exceptions import OpsworksScalingException

LOGGER = logging.getLogger(__name__)

class AutoscaleException(OpsworksScalingException):
    pass

class Autoscaler(object):

    def __init__(self, controller, config, pause_file_dir=None):
        self.controller = controller
        self.config = config

        if pause_file_dir is None:
            pause_file_dir = os.path.expanduser('~')
        self.pause_file = os.path.join(pause_file_dir, '.moscaler-pause')

    @property
    def up_increment(self):
        return self.config['up_increment']

    @property
    def down_increment(self):
        return self.config['down_increment']

    @property
    def pause_cycles(self):
        return self.config['pause_cycles']

    @property
    def strategies(self):
        return self.config['strategies']

    def pause_scaling(self, cycles):
        LOGGER.debug("Updating %s to indicate %d pause cycles",
                     self.pause_file, cycles)
        self._write_pause_file(cycles)

    def scaling_paused(self):
        pause_cycles = self._read_pause_file()
        LOGGER.info("Pause cycles remaining: %d", pause_cycles)
        if pause_cycles > 0:
            LOGGER.info("Scaling paused")
            return True
        else:
            LOGGER.info("Scaling ok")
            return False

    def _tick_pause_cycles(self):
        remaining_cycles = self._read_pause_file() - 1
        if remaining_cycles < 1 and os.path.exists(self.pause_file):
            os.unlink(self.pause_file)
        else:
            self._write_pause_file(remaining_cycles)

    def _read_pause_file(self):
        if not os.path.exists(self.pause_file):
            LOGGER.debug("Pause file %s does not exist", self.pause_file)
            return 0
        with open(self.pause_file, 'rb') as f:
            try:
                return int(f.read())
            except ValueError:
                LOGGER.warning("Failed reading (stale?) pause file")
                return 0

    def _write_pause_file(self, cycles):
        with open(self.pause_file, 'wb') as f:
            f.write(str(cycles))

    def execute(self):

        results = {}
        for strategy in self.strategies:
            try:
                method = getattr(self, strategy['method'])
                direction = method(strategy['settings'])

            except AttributeError:
                raise OpsworksScalingException(
                    "No such autoscale method: '%s'", strategy['method']
                )

            if direction is None:
                LOGGER.info("%s indicates no action", strategy['name'])

            LOGGER.info("%s says: '%s'", strategy['name'], direction)
            results[strategy['name']] = direction

        self._scale_up_or_down(results)

    def _scale_up_or_down(self, results):

        # only one has to say 'up' to go up
        if 'up' in results.values() and not self.scaling_paused():
            self.controller._scale_up(self.up_increment, scale_available=True)
            if self.pause_cycles:
                self.pause_scaling(self.pause_cycles)
            # return here to avoid ticking the pause cycle value
            return

        # everyone has to agree to go down
        elif results and all(d == 'down' for d in results.values()):
            with self.controller.mhorn.in_maintenance(
                    self.controller.online_workers,
                    dry_run=self.controller.dry_run):
                self.controller._scale_down(
                    self.down_increment,
                    check_uptime=True,
                    scale_available=True
                )

        self._tick_pause_cycles()

    @property
    def cw(self):
        if not hasattr(self, '_cw'):
            self._cw = boto3.client('cloudwatch')
        return self._cw

    def cloudwatch(self, settings):
        """
        determines 'up' or 'down' based on the cloudwatch metric data for
        an opsworks cluster layer
        """

        try:
            metric = settings['metric']
            namespace = settings['namespace']
            up_threshold = settings['up_threshold']
            down_threshold = settings.get('down_threshold')
            sample_count = settings.get('sample_count', 3)
            sample_period = settings.get('sample_period', 60)
            up_threshold_online_workers_multiplier = settings.get('up_threshold_online_workers_multiplier', 0)
        except KeyError, e:
            raise AutoscaleException("Invalid settings for metric autoscaling: %s" % str(e))

        if 'layer_name' in settings:
            layer_name = settings['layer_name']
            dimensions = {
                'Name': 'LayerId',
                'Value': self.controller.get_layer_id(layer_name)
            }
            LOGGER.debug("Fetching recent datapoints for metric %s on layer '%s'",
                         metric, layer_name)

        elif 'instance_name' in settings:
            instance_name = settings['instance_name']
            dimensions = {
                'Name': 'InstanceId',
                'Value': self.controller.get_ec2_id(instance_name)
            }
            LOGGER.debug("Fetching recent datapoints for metric %s on instance '%s'",
                         metric, instance_name)

        else:
            raise AutoscaleException("strategy settings must specify one of "
                                     "'layer_name' or 'instance_name'")

        start_time = datetime.utcnow() - timedelta(seconds=600)
        end_time = datetime.utcnow()

        resp = self.cw.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric,
            Dimensions=[dimensions],
            StartTime=start_time,
            EndTime=end_time,
            Period=sample_period,
            Statistics=['Average']
        )

        datapoints = sorted(resp['Datapoints'],
                            key=itemgetter('Timestamp'),
                            reverse=True
                            )

        LOGGER.debug("Most recent datapoint is %d seconds old",
                     (datetime.utcnow() - datapoints[0]['Timestamp'].replace(tzinfo=None)).seconds)

        datapoints = [x['Average'] for x in datapoints[:sample_count]]
        LOGGER.debug("Datapoints for %s: %s", metric, datapoints)

        up_threshold += len(self.controller.online_workers) * up_threshold_online_workers_multiplier
        LOGGER.debug("Adjusted scale up threshold by online workers * %d: %f",
                     up_threshold_online_workers_multiplier, up_threshold)

        return self._up_or_down(datapoints, up_threshold, down_threshold)

    def queued_jobs(self, settings):
        """
        'up' or 'down' based on the number of queued jobs reported by
        Matterhorn. Settings can specify particular operation types to look at.
        NOTE: it is prefered to publish the queued jobs count as a cloudwatch
        metric that can be used with the cloudwatch() method as that allows for
        sampling > 1 datapoint.
        """

        try:
            up_threshold = settings['up_threshold']
            down_threshold = settings['down_threshold']
            operation_types = settings.get('operation_types')
        except KeyError, e:
            raise AutoscaleException("Invalid settings for queued_jobs autoscaling: %s" % str(e))

        if operation_types is not None:
            LOGGER.debug("Checking for queued jobs of types: %s", str(operation_types))

        queued_jobs = self.controller.mhorn.queued_job_count(
            operation_types=operation_types
        )

        LOGGER.info("MH reports %d queued jobs", queued_jobs)

        return self._up_or_down([queued_jobs], up_threshold, down_threshold)

    def _up_or_down(self, datapoints, up_threshold, down_threshold):

        if datapoints and all(x >= up_threshold for x in datapoints):
            LOGGER.debug("scale up threshold met")
            return 'up'

        elif datapoints and all(x < down_threshold for x in datapoints):
            LOGGER.debug("scale down threshold met")
            return 'down'

