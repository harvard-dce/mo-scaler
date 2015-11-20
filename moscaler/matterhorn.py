
import pyhorn
# this is a hack until pyhorn can get it's caching controls sorted out
pyhorn.client._session._is_cache_disabled = True

import logging
from stopit import SignalTimeout, \
    TimeoutException as StopitTimeout
from requests.exceptions import \
    Timeout as RequestsTimeout, \
    ConnectionError

from contextlib import contextmanager
from os import getenv as env
from moscaler.exceptions import MatterhornCommunicationException

LOGGER = logging.getLogger(__name__)

PYHORN_TIMEOUT = 30
URI_SCHEME = 'http'
HIGH_LOAD_JOB_TYPES = ["compose", "editor", "inspect", "video-segment"]


class MatterhornController(object):

    def __init__(self, host):

        self.mh_url = "%s://%s" % (URI_SCHEME, host)
        self.client = pyhorn.MHClient(
            self.mh_url,
            user=env('MATTERHORN_USER'),
            passwd=env('MATTERHORN_PASS'),
            timeout=env('PYHORN_TIMEOUT', PYHORN_TIMEOUT)
        )

        try:
            self.verify_connection()
            self.refresh_stats()
            self._online = True
        except MatterhornCommunicationException as exc:
            LOGGER.warning("Matterhorn connection failure: %s", str(exc))
            self._online = False

    def __repr__(self):
        return "%s (%s)" % (self.__class__, self.mh_url)

    def verify_connection(self):
        try:
            LOGGER.debug("verifying pyhorn client connection")
            with SignalTimeout(5, swallow_exc=False):
                assert self.client.me() is not None
        except (ConnectionError, RequestsTimeout, StopitTimeout) as exc:
            raise MatterhornCommunicationException(
                "Error connecting to Matterhorn API at {}: {}".format(
                    self.mh_url, str(exc)
                )
            )

    def is_online(self):
        return self._online

    def refresh_stats(self):
        self._hosts = self.client.hosts()
        self._stats = self.client.statistics()

    def job_status(self):
        status = {
            'queued_jobs': self.queued_job_count(),
            'queued_jobs_high_load': self.queued_high_load_job_count()
        }
        if self.is_online():
            status['running_jobs'] = self._stats.running_jobs()
        else:
            status['running_jobs'] = 0

        return status

    def node_status(self, inst):
        if not self.is_online():
            return {}
        if not self.is_registered(inst):
            return {'registered': False}
        return {
            'registered': self.is_registered(inst),
            'maintenance': self.is_in_maintenance(inst),
            'idle': self.is_idle(inst)
        }

    def queued_high_load_job_count(self):
        return self.queued_job_count(operation_types=HIGH_LOAD_JOB_TYPES)

    def queued_job_count(self, operation_types=None):

        if not self.is_online():
            return 0

        # get the running workflows; high "count" value to make sure we get all
        running_wfs = self.client.workflows(state="RUNNING", count=1000)

        # then get their running operations
        running_ops = []
        for wkf in running_wfs:
            running_ops.extend(
                [x for x in wkf.operations
                 if x.state in ['RUNNING', 'WAITING']]
            )

        # filter for the operation types we're interested in
        if operation_types is not None:
            running_ops = [x for x in running_ops if x.id in operation_types]

        # now get any queued child jobs of those operations
        queued_jobs = []
        for opr in running_ops:
            queued_jobs.extend(
                [x for x in opr.job.children if x.status == 'QUEUED']
            )

        return len(queued_jobs)

    def is_registered(self, inst):
        registered = [x.base_url for x in self._hosts]
        return hasattr(inst, 'mh_host_url') and inst.mh_host_url in registered

    def get_host(self, inst):

        try:
            return next(x for x in self._hosts
                        if x.base_url == inst.mh_host_url
                        )
        except StopIteration:
            return None

    def is_idle(self, inst):
        running_jobs = self._stats.running_jobs(inst.mh_host_url)
        LOGGER.debug("%r has %d running jobs", inst, running_jobs)
        return running_jobs == 0

    def filter_idle(self, instances):
        self.refresh_stats()
        return [x for x in instances if self.is_idle(x)]

    def is_in_maintenance(self, inst):
        host = self.get_host(inst)
        return host.maintenance

    def maintenance_off(self, inst):
        host = self.get_host(inst)
        LOGGER.debug("Setting maintenance to off for %r", inst)
        host.set_maintenance(False)

    def maintenance_on(self, inst):
        host = self.get_host(inst)
        LOGGER.debug("Setting maintenance to on for %r", inst)
        host.set_maintenance(True)

    @contextmanager
    def in_maintenance(self, instances, restore_state=True, dry_run=False):
        """Context manager for ensuring matterhorn nodes are in maintenance
        state while performing operations"""

        LOGGER.debug("Ensuring workers in maintenance")

        # only deal with nodes that are not already in maintenance
        for_maintenance = [x for x in instances
                           if self.is_registered(x)
                           and not self.is_in_maintenance(x)
                           ]

        if not len(for_maintenance):
            # don't do anything
            yield
        else:
            try:
                for inst in for_maintenance:
                    LOGGER.debug("Enabling maintenance mode for %r", inst)
                    if not dry_run:
                        self.maintenance_on(inst)
                self.refresh_stats()
                yield  # let calling code do it's thing
            except Exception as exc:
                LOGGER.debug(
                    "Exception caught during 'in_maintenance' context: %s, %s",
                    type(exc),
                    str(exc)
                )
                raise
            finally:
                if restore_state:
                    LOGGER.debug("Restoring maintenance state")
                    for inst in for_maintenance:
                        if inst.action_taken == 'stopped':
                            LOGGER.debug(
                                "Not unsetting maintenance for stopped: %r",
                                inst
                            )
                        else:
                            LOGGER.debug("Disabling maintenance for %r", inst)
                            if not dry_run:
                                self.maintenance_off(inst)
                    self.refresh_stats()
