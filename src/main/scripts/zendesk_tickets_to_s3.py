#!/usr/bin/env python

import logging

from blinkhealth_zendesk.cmd_arg import get_argparser
from blinkhealth_zendesk.consumer import IncrementalTicketConsumer
from blink_commons import writeZenDataToS3

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    account, token, bucket = get_argparser()

    consumer = IncrementalTicketConsumer(account, token)
    try:
        for df in consumer.read():
            #print(len(df))
            writeZenDataToS3(bucket, df, 'tickets')
    except KeyboardInterrupt:
        consumer.checkpoint()
