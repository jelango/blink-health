import logging
import os
import pickle
import time
from collections import OrderedDict
from datetime import datetime

import pandas as pd
import requests

BASE_URL = 'https://blinkhealth.zendesk.com/api/v2'
STATE_PATH = '/tmp/zendesk-consumer-state-{}.pkl'
MIN_INCREMENTAL_EXPORT_AGE_SECONDS = 600
MAX_INCREMENTAL_EXPORT_BATCH_SIZE = 1000


class ZendeskConsumer(object):
    def __init__(self, url, account, token, data_key, fields_to_keep, timestamp_fields=['created_at', 'updated_at']):
        self.state_path = STATE_PATH.format(self.__class__.__name__)
        logging.info('Keeping state in %s', self.state_path)

        self.url = url
        self.account = account
        self.token = token
        self.data_key = data_key
        self.fields_to_keep = fields_to_keep
        self.timestamp_fields = timestamp_fields
        self.next_page, self.end_time, self.incomplete_batch = self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_path):
            state = pickle.load(open(self.state_path, 'rb'))
        else:
            state = {
                'next_page': None,
                'end_time': 0,
                'incomplete_batch': []
            }
        return state['next_page'], state['end_time'], state['incomplete_batch']

    def _store_state(self):
        state = {
            'next_page': self.next_page,
            'end_time': self.end_time,
            'incomplete_batch': self.incomplete_batch
        }
        logging.debug('Storing current state {}'.format(state))
        pickle.dump(state, open(self.state_path, 'wb'))

    def checkpoint(self, incomplete_batch):
        """
        Store current export state, i.e. timestamp and counts and incomplete dataset.
        Incomplete dataset is the dataset for the last hour,
        as it is unknown if there is anything left for the last hour in the next export batch.
        """
        self.incomplete_batch = incomplete_batch
        self._store_state()

    def get(self, url):
        """
        Make requests to Zendesk API.
        If rate limit hit, then waits for the recommended number of seconds, based on the response header, then re-tries.
        :param url:
        :return:
        """
        response = requests.get(url, auth=('{}/token'.format(self.account), self.token))

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

            next_page = self.next_page or self.url
            headers, data = self.get(next_page)

            done = data['count'] < MAX_INCREMENTAL_EXPORT_BATCH_SIZE

            self.next_page = data['next_page']
            self.end_time = data['end_time']

            if self.fields_to_keep:
                filtered_data = [{key: d[key] for key in self.fields_to_keep} for d in data[self.data_key]]
            else:
                filtered_data = data[self.data_key]

            # whatever else pre-processing of the data should happen here

            df = pd.DataFrame.from_records(filtered_data)
            for field in self.timestamp_fields:
                df[field] = pd.to_datetime(df[field])

            if self.incomplete_batch:
                # preprend whatever was available from the previous read
                df = pd.DataFrame.from_records(self.incomplete_batch).append(df)
                self.incomplete_batch = []

            yield df


class IncrementalTicketConsumer(ZendeskConsumer):
    def __init__(self, account, token):
        super().__init__('{}/incremental/tickets.json?start_time=0&include=users'.format(BASE_URL),
                         account,
                         token,
                         'tickets',
                         ['id',
                          'external_id',
                          'created_at',
                          'updated_at',
                          'type',
                          'subject',
                          'description',
                          'priority',
                          'status'])


class IncrementalCallConsumer(ZendeskConsumer):
    def __init__(self, account, token):
        super().__init__('{}/channels/voice/incremental/calls.json?start_time=0&include=users'.format(BASE_URL),
                         account,
                         token,
                         'calls',
                         [])
