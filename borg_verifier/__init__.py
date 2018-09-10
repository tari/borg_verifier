import logging
import socket

from borg_verifier.verifier import BorgVerifier
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import CollectorRegistry, Gauge, exposition, push_to_gateway


logger = logging.getLogger(__name__)


def run(pushgateway, repos, auth_credentials=None):
    registry = CollectorRegistry()
    # Repository ID and location, with result:
    #  * OK if everything is okay
    #  * BAD if there was a check error
    #  * INTERNAL if there was a problem running things
    check_complete_counter = Gauge(
            'borg_last_verify',
            'Last time the repository was checked',
            ['repo', 'result'],
            registry=registry)
    snapshot_created_counter = Gauge(
            'borg_snapshot_creation_time',
            'Time each snapshot in the repository was created',
            ['repo', 'snapshot'],
            registry=registry)
    # Kind is one of 'raw', 'compressed_raw', 'unique' or 'compressed_unique'
    repo_size_counter = Gauge(
            'borg_repository_size',
            'Size of the repository, in bytes',
            ['repo', 'kind'],
            registry=registry)

    def run_verifier(repo_path):
        logger.debug("Run verifier for %s", repo_path)
        verifier = BorgVerifier(repo_path, check_complete_counter,
                                snapshot_created_counter, repo_size_counter)
        verifier.verify_and_export()

    executor = ThreadPoolExecutor()
    results = executor.map(run_verifier, repos)
    while True:
        try:
            next(results)
        except StopIteration:
            break
        except Exception as e:
            logger.error('Caught exception from executor: %s', str(e))

    logger.info('Pushing metrics to %s', pushgateway)
    grouping_key = {
        'instance': socket.gethostname(),
    }
    if auth_credentials is not None:
        (username, password) = auth_credentials

        def auth_handler(url, method, timeout, headers, data):
            return exposition.basic_auth_handler(
                    url, method, timeout, headers, data, username, password)
        push_to_gateway(pushgateway, grouping_key=grouping_key, job='borg_verifier',
                        registry=registry, handler=auth_handler)
    else:
        push_to_gateway(pushgateway, grouping_key=grouping_key,
                        job='borg_verifier', registry=registry)
