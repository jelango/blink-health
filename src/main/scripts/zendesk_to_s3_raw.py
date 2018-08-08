#!/usr/bin/env python
import json
import logging
import os

from blinkhealth_zendesk.args import get_args
from blinkhealth_zendesk.aws_common import put_to_stream

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    consumer, data_url = get_args()
    try:
        for data in consumer.read():

            for dataItem in consumer.process_raw(data):
                jsondata = json.dumps(dataItem)
                #with open('/tmp/zendesk_{}.json'.format(consumer.consumer_type()), mode='w') as fp:
                    #fp.write(jsondata)
                put_to_stream("zen_{}".format(consumer.consumer_type()), jsondata)
            consumer.checkpoint()
    finally:
        consumer.checkpoint()
