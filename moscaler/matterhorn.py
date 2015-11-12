
import pyhorn
# this is a hack until pyhorn can get it's caching controls sorted out
pyhorn.client._session._is_cache_disabled = True

import stopit
import logging
from requests.exceptions import Timeout as RequestsTimeout
from contextlib import contextmanager
from os import getenv as env
from exceptions import *

log = logging.getLogger(__name__)

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
            self.refresh_hosts()
            self._online = True
        except MatterhornCommunicationException, e:
            log.warning("Matterhorn connection failure: %s", str(e))
            self._online = False

    def __repr__(self):
        return "%s (%s)" % (self.__class__, self.mh_url)

    def verify_connection(self):
        try:
            log.debug("verifying pyhorn client connection")
            with stopit.SignalTimeout(5, swallow_exc=False):
                assert self.client.me() is not None
        except (RequestsTimeout, stopit.TimeoutException), e:
            # this could be anything: communication problem, unexpected response, etc
            raise MatterhornCommunicationException(
                "Error connecting to Matterhorn API at {}: {}".format(
                    self.mh_url, str(e)
                )
            )

    def is_online(self):
        return self._online

    def refresh_hosts(self):
        self._hosts = self.client.hosts()

    def job_status(self):
        status = {
            'queued_jobs': self.queued_job_count(),
            'queued_jobs_high_load': self.queued_job_count(
                operation_types=HIGH_LOAD_JOB_TYPES
            )
        }
        if self.is_online():
            stats = self.client.statistics()
            status['running_jobs'] = stats.running_jobs()
        else:
            status['running_jobs'] = 0

        return status

    def queued_job_count(self, operation_types=None):

        if not self.is_online():
            return 0

        # get the running workflows; high "count" value to make sure we get all
        running_wfs = self.client.workflows(state="RUNNING", count=1000)

        # then get their running operations
        running_ops = []
        for wf in running_wfs:
            running_ops.extend(filter(
                lambda x: x.state in ["RUNNING","WAITING"],
                wf.operations
            ))

        # filter for the operation types we're interested in
        if operation_types is not None:
            running_ops = filter(lambda x: x.id in operation_types, running_ops)

        # now get any queued child jobs of those operations
        queued_jobs = []
        for op in running_ops:
            queued_jobs.extend(filter(lambda x: x.status == "QUEUED", op.job.children))

        return len(queued_jobs)

    def get_host_by_url(self, host_url):

        try:
            return next(h for h in self._hosts
                        if h.base_url == 'http://' + host_url)
        except StopIteration:
            raise MatterhornNodeException(
                "No Matterhrorn node found for %s" % host_url
            )

    def filter_idle(self, instances):
        stats = self.client.statistics()
        def is_idle(inst):
            running_jobs = stats.running_jobs('http://' + inst.PrivateDns)
            log.debug("%r has %d running jobs", inst, running_jobs)
            return running_jobs == 0
        return [x for x in instances if is_idle(x)]

    def is_in_maintenance(self, inst):
        host = self.get_host_by_url(inst.PrivateDns)
        return host.maintenance

    def maintenance_off(self, inst):
        host = self.get_host_by_url(inst.PrivateDns)
        log.debug("Setting maintenance to off for %r", inst)
        host.set_maintenance(False)

    def maintenance_on(self, inst):
        host = self.get_host_by_url(inst.PrivateDns)
        log.debug("Setting maintenance to on for %r", inst)
        host.set_maintenance(True)

    @contextmanager
    def in_maintenance(self, instances, restore_state=True):
        """Context manager for ensuring matterhorn nodes are in maintenance
        state while performing operations"""

        log.debug("Ensuring instances in maintenance: %s",
                  ', '.join(repr(x) for x in instances))

        # make sure we're only dealing with nodes that are not already in maintenance
        for_maintenance = [x for x in instances if not self.is_in_maintenance(x)]

        if not len(for_maintenance):
            # don't do anything
            yield
        else:
            try:
                for inst in for_maintenance:
                    self.maintenance_on(inst)
                    log.debug("Maintenace mode set for %r" % inst)
                self.refresh_hosts()
                yield # let calling code do it's thing
            except Exception, e:
                log.debug("Exception caught during 'in_maintenance' context: %s, %s", type(e), str(e))
                raise
            finally:
                if restore_state:
                    for inst in for_maintenance:
                        if not inst.is_online():
                            log.debug("not unsetting maintenance for %r as it was stopped", inst)
                        else:
                            self.maintenance_off(inst)
                            log.debug("Maintenance mode unset for %r" % inst)
                    self.refresh_hosts()
