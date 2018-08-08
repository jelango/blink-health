#!/usr/bin/env python
import json
import logging
import os

from blinkhealth_zendesk.args import get_args

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    consumer, data_url = get_args()
    try:
        for data in consumer.read():
            with open('/tmp/zendesk_{}.json'.format(consumer.consumer_type())) as fp:
                json.dump(fp, data)
            consumer.checkpoint()
    finally:
        consumer.checkpoint()
