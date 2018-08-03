#!/usr/bin/env python

import logging

from blinkhealth_zendesk.cmd_arg import get_argparser
from blinkhealth_zendesk.consumer import IncrementalCallConsumer
from blink_commons import writeZenDataToS3


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    account, token, bucket = get_argparser()

    consumer = IncrementalCallConsumer(account, token)
    try:
        for df in consumer.read():
            #print(df[100])
            writeZenDataToS3(bucket, df,  'calls')
    except KeyboardInterrupt:
        consumer.checkpoint()
