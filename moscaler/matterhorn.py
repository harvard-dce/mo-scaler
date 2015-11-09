
import pyhorn
# this is a hack until pyhorn can get it's caching controls sorted out
pyhorn.client._session._is_cache_disabled = True

import logging
from os import getenv as env
from exceptions import *

log = logging.getLogger(__name__)

DEFAULT_PYHORN_TIMEOUT = 30

class MatterhornController():

    @classmethod
    def client_factory(cls, api_url):
        client = pyhorn.MHClient(
            api_url,
            user=env('MATTERHORN_USER'),
            passwd=env('MATTERHORN_PASS'),
            timeout=env('PYHORN_TIMEOUT', DEFAULT_PYHORN_TIMEOUT)
        )

        try:
            log.debug("verifying pyhorn client connection")
            assert client.me() is not None
            return client
        except Exception, e:
            # this could be anything: communication problem, unexpected response, etc
            log.debug("pyhorn client failed to connect")
            raise MatterhornCommunicationException(
                "Error connecting to Matterhorn API at {}: {}".format(
                    api_url, str(e)
                )
            )

    def __init__(self, mh_url):
        self.client = MatterhornController.client_factory(mh_url)
