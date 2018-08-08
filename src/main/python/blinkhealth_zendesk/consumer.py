import abc
import logging
import os
import pickle
import time
from collections import OrderedDict
from datetime import datetime
from enum import Enum

import pandas as pd
import requests

from blinkhealth_zendesk.aws_common import write_to_s3, read_from_s3

BASE_URL = 'https://blinkhealth.zendesk.com/api/v2'
MIN_INCREMENTAL_EXPORT_AGE_SECONDS = 300
MAX_INCREMENTAL_EXPORT_BATCH_SIZE = 1000


class ZendeskConsumerType(Enum):
    tickets = 1
    calls = 2

    def __str__(self):
        return self.name

    def to_consumer(self, configuration_url, account, token):
        if self == self.tickets:
            return IncrementalTicketConsumer(configuration_url, account, token)

        if self == self.calls:
            return IncrementalCallConsumer(configuration_url, account, token)

        raise ValueError('Unexpected consumer type %s', self)


class ZendeskConsumer(object):
    def __init__(self, configuration_url, api_url, account, token):
        self.state_path = '{}/zendesk-consumer-state-{}.json'.format(configuration_url, self.__class__.__name__)
        logging.info('Keeping state in %s', self.state_path)

        self.api_url = api_url
        self.account = account
        self.token = token
        self.next_page, self.end_time = self._load_state()

    @abc.abstractmethod
    def consumer_type(self):
        pass

    def _load_state(self):
        default_state = {
            'next_page': None,
            'end_time': 0
        }
        if self.state_path.startswith('s3'):
            state = read_from_s3(self.state_path, default_state)
        else:
            if os.path.exists(self.state_path):
                state = pickle.load(open(self.state_path, 'rb'))
            else:
                state = default_state

        return state['next_page'], state['end_time']

    def _store_state(self):
        state = {
            'next_page': self.next_page,
            'end_time': self.end_time
        }
        logging.debug('Storing current state {}'.format(state))

        if self.state_path.startswith('s3'):
            write_to_s3(self.state_path, state)
        else:
            pickle.dump(state, open(self.state_path, 'wb'))

    def checkpoint(self):
        """
        Store current export state, i.e. timestamp and counts.
        """
        self._store_state()

    def get(self, url):
        """
        Make requests to Zendesk API.
        If rate limit hit, then waits for the recommended number of seconds, based on the response header, then re-tries.
        :param url:
        :return:
        """
        response = requests.get(url, auth=('{}/token'.format(self.account), self.token), timeout=15)

        if response.status_code == 429:
            logging.warning('Rate limit hit, retrying after: %s', response.headers['Retry-After'])
            time.sleep(response.headers['Retry-After'])
            return self.get(url)
        if response.status_code != 200:
            raise ValueError('{}: {}'.format(response.status_code, response.text))

        return response.headers, response.json(object_pairs_hook=OrderedDict)

    def read(self):
        """
        Reads next batch of records from Zendesk incremental API.
        If API was hit recently (within 600 seconds, according to Zendesk API), waits for the difference, then re-tries.
        :return: list of records (dictionaries)
        """
        done = False
        while not done:
            req_age_seconds = datetime.now().timestamp() - self.end_time
            if req_age_seconds < MIN_INCREMENTAL_EXPORT_AGE_SECONDS:
                logging.warning(
                    'Too early to request data export, %s instead of minimal %s seconds difference, waiting...',
                    req_age_seconds, MIN_INCREMENTAL_EXPORT_AGE_SECONDS)
                time.sleep(req_age_seconds)

            next_page = self.next_page or self.api_url
            headers, data = self.get(next_page)

            done = data['count'] < MAX_INCREMENTAL_EXPORT_BATCH_SIZE

            self.next_page = data['next_page']
            self.end_time = data['end_time']

            yield data


class IncrementalTicketConsumer(ZendeskConsumer):
    def __init__(self, configuration_url, account, token):
        super().__init__(configuration_url,
                         '{}/incremental/tickets.json?start_time=0&include=users'.format(BASE_URL),
                         account,
                         token)

    def consumer_type(self):
        return str(ZendeskConsumerType.tickets)


class IncrementalCallConsumer(ZendeskConsumer):
    def __init__(self, configuration_url, account, token):
        super().__init__(configuration_url,
                         '{}/channels/voice/incremental/calls.json?start_time=0&include=users'.format(BASE_URL),
                         account,
                         token)

    def consumer_type(self):
        return str(ZendeskConsumerType.calls)
