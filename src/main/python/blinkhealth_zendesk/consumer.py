import logging
import os
import pickle
import time
from datetime import datetime

import requests

BASE_URL = 'https://blinkhealth.zendesk.com/api/v2'
STATE_PATH = '/tmp/zendesk-consumer-state-{}.pkl'


class ZendeskConsumer(object):
    def __init__(self, url, data_key, fields_to_keep, account, token):
        self.state_path = STATE_PATH.format(self.__class__.__name__)
        logging.info('Keeping state in %s', self.state_path)

        self.url = url
        self.data_key = data_key
        self.fields_to_keep = fields_to_keep
        self.account = account
        self.token = token
        self.next_page, self.end_time = self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_path):
            state = pickle.load(open(self.state_path, 'rb'))
        else:
            state = {
                'next_page': None,
                'end_time': 0,
            }
        return state['next_page'], state['end_time']

    def _store_state(self):
        state = {
            'next_page': self.next_page,
            'end_time': self.end_time,
        }
        logging.debug('Storing current state {}'.format(state))
        pickle.dump(state, open(self.state_path, 'wb'))

    def checkpoint(self):
        self._store_state()

    def get(self, url):
        response = requests.get(url, auth=('{}/token'.format(self.account), self.token))

        if response.status_code == 429:
            logging.warning('Rate limit hit, retrying after: %s', response.headers['Retry-After'])
            time.sleep(response.headers['Retry-After'])
            return self.get(url)
        if response.status_code != 200:
            raise ValueError('{}: {}'.format(response.status_code, response.text))

        return response.headers, response.json()

    def read(self):
        done = False
        while not done:

            #TBD needs Ilgar to verify these commented lines
            #req_age_seconds = datetime.now().timestamp() - self.end_time
            #if req_age_seconds < 600:
            #    logging.debug(
            #        'Too early to request data export, %s instead of minimal %s seconds difference, waiting...',
            #        req_age_seconds, 600)
            #    time.sleep(req_age_seconds)



            next_page = self.next_page or self.url
            headers, data = self.get(next_page)

            # TBD needs Ilgar to verify this commented line
            #done = data['count'] < 1000

            self.next_page = data['next_page']
            self.end_time = data['end_time']

            if self.fields_to_keep:
                yield [{key: d[key] for key in self.fields_to_keep} for d in data[self.data_key]]
            else:
                yield data[self.data_key]


            ## TBD needs Ilgar to verify this added line
            logging.debug('Sleep 60 secs..')
            time.sleep(60)


class IncrementalTicketConsumer(ZendeskConsumer):
    def __init__(self, account, token):
        super().__init__('{}/incremental/tickets.json?start_time=0&include=users'.format(BASE_URL),
                         'tickets',
                         ['id',
                          'external_id',
                          'created_at',
                          'updated_at',
                          'type',
                          'subject',
                          'description',
                          'priority',
                          'status'],
                         account,
                         token)


class IncrementalCallConsumer(ZendeskConsumer):
    def __init__(self, account, token):
        super().__init__('{}/channels/voice/incremental/calls.json?start_time=0&include=users'.format(BASE_URL),
                         'calls',
                         [],
                         account,
                         token)
