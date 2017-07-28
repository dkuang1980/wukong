from wukong.zookeeper import Zookeeper
from wukong.errors import SolrError
import requests
import random
import json
import time
import logging

log = logging.getLogger(__name__)

try:
    from urlparse import urljoin
except:
    from urllib.parse import urljoin


def process_response(response):
    if response.status_code != 200:
        raise SolrError(response.reason)
    try:
        response_content = json.loads(response.text)
    except:
        raise SolrError("Parsing Error: %s" % response.text)

    return response_content


class SolrRequest(object):
    """
    Handle requests to SOLR and response from SOLR
    """
    def __init__(self, solr_hosts, zookeeper_hosts=None, timeout=15):
        self.client = requests.Session()
        self.current_hosts = solr_hosts
        self.timeout = timeout
        self.bad_hosts = []  # tuples of (host, time) to avoid
        if zookeeper_hosts is not None:
            self.zookeeper = Zookeeper(zookeeper_hosts)
        else:
            self.zookeeper = None
        # time to revert to old host list (in minutes) after an error
        self.check_hosts = 5

    def _request(self, method, host, path, params, body, timeout):
        headers = {'content-type': 'application/json'}

        extraparams = {'wt': 'json',
                       'omitHeader': 'true',
                       'json.nl': 'map'}

        if params is None:
            params = {}

        params.update(extraparams)

        fullpath = urljoin(host, path)

        response = self.client.request(
            method,
            fullpath,
            params=params,
            headers=headers,
            data=body,
            timeout=timeout
        )

        return process_response(response)

    def get_next_host(self):
        current_hosts = set(self.current_hosts)

        def is_still_bad(htime):
            return (time.time() - htime) / 60 < self.check_hosts

        self.bad_hosts = [(host, htime) for host, htime in
                          self.bad_hosts if is_still_bad(htime)]

        bad_hosts = set([h for h, _ in self.bad_hosts])
        available = list(current_hosts - bad_hosts)
        random.shuffle(available)
        if available:
            return available[0]

        elif self.bad_hosts:
            oldest = min(self.bad_hosts, key=lambda x: x[1])
            self.bad_hosts.remove(oldest)
            return oldest[0]

        return None

    def request(self, path, params, method, body=None, is_retry=False):
        """
        Prepare data and send request to SOLR servers
        """
        host = self.get_next_host()

        try:
            if host is None:
                raise SolrError('Cannot find a host')

            return self._request(
                method=method,
                host=host,
                path=path,
                params=params,
                body=body,
                timeout=self.timeout
            )

        except Exception as e:
            log.exception(
                "An wukong exception occurred while executing the"
                " following a request: \n host: {} \nmethod: {} \npath: {}"
                " \nparams: {} \nbody: {}".format(
                    host, method, path, params, body
                )
            )
            log.warn("Marking {} as a bad host for {} min"
                     .format(host, self.check_hosts))
            self.bad_hosts.append((host, time.time()))

            if self.zookeeper is not None:
                available_hosts = self.zookeeper.get_active_hosts()
                available_hosts = ["http://%s/solr/" % h
                                   for h in available_hosts]
                self.current_hosts = available_hosts

            if not is_retry:
                return self.request(path, params, method, body, is_retry=True)
            else:
                raise SolrError(str(e))

    def post(self, path, params=None, body=None):
        """
        Send a POST request to the SOLR servers
        """
        return self.request(path, params, 'POST', body=body)

    def get(self, path, params=None):
        """
        Send a GET request to the SOLR servers
        """
        return self.request(path, params, 'GET')
