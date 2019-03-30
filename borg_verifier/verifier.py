#!/usr/bin/env python3

import calendar
import json
import logging
import os
import subprocess
from datetime import datetime


logger = logging.getLogger(__name__)


class BorgVerifier(object):
    def __init__(self, repository_path, check_complete_counter,
                 snapshot_created_counter, repo_size_counter):
        self.check_complete_counter = check_complete_counter
        self.snapshot_created_counter = snapshot_created_counter
        self.repo_size_counter = repo_size_counter

        self.path = repository_path

    def borg(self, *args):
        env = os.environ.copy()
        env.update({
            'BORG_REPO': self.path,
            'BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK': 'yes',
            'BORG_RELOCATED_REPO_ACCESS_IS_OK': 'yes',
        })
        logger.debug('Invoking borg with extra args %s', args)
        p = subprocess.run(
                ('borg', '--lock-wait', '900') + args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=None,
                check=True,
                env=env)
        return p.stdout

    def borg_json(self, *args):
        return json.loads(self.borg(*(args + ('--json',))))

    def _set_complete(self, status):
        self.check_complete_counter.labels(
                repo=self.path, result=status).set_to_current_time()

    def _log(self, level, msg, *args, **kwargs):
        logger.log(level, '%s: ' + msg, self.path, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log(logging.INFO, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log(logging.ERROR, msg, *args, **kwargs)

    SIZE_LABELS = {
        'total_size': 'raw',
        'total_csize': 'compressed_raw',
        'unique_size': 'unique',
        'unique_csize': 'compressed_unique',
    }

    def verify_and_export(self):
        try:
            self.info('Begin check')
            self.borg('check')
            self.info('Completed check')

            self.info('Get info')
            info = self.borg_json('info')
            if 'cache' in info:
                stats = info['cache']['stats']
                for prop, label in self.SIZE_LABELS.items():
                    value = int(stats[prop])
                    self.info('%s size: %d', label, value)
                    self.repo_size_counter.labels(
                            repo=self.path, kind=label).set(value)

            self.info('Get snapshot creation times')
            for archive in self.borg_json('list')['archives']:
                # Borg uses ISO 8601 in UTC, but without a time zone identifier.
                start_time = datetime.strptime(
                        archive['start'], '%Y-%m-%dT%H:%M:%S.%f')
                start_timestamp = calendar.timegm(start_time.utctimetuple())
                snapshot_id = archive['id']
                self.info('%s created %d', snapshot_id, start_timestamp)
                self.snapshot_created_counter.labels(
                        repo=self.path, snapshot=snapshot_id).set(start_timestamp)

            # All done
            self.info('OK')
            self._set_complete('OK')
        except subprocess.CalledProcessError:
            self.error('BAD')
            self._set_complete('BAD')
        except Exception:
            self.error('ERROR')
            self._set_complete('INTERNAL')
            raise
