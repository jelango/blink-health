#!/usr/bin/env python

import logging

from blinkhealth_zendesk.cmd_arg import get_argparser
from blinkhealth_zendesk.consumer import IncrementalCallConsumer

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    account, token = get_argparser()

    consumer = IncrementalCallConsumer(account, token)
    try:
        for df in consumer.read():
            print(df[100])
    except KeyboardInterrupt:
        consumer.checkpoint()
