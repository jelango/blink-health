#!/usr/bin/env python

import logging
import os

import pandas as pd

from blinkhealth_zendesk.args import get_args


def clean_to_df(data, data_key, timestamp_fields, fields_to_keep):
    if fields_to_keep:
        filtered_data = [{key: d[key] for key in fields_to_keep} for d in data[data_key]]
    else:
        filtered_data = data[data_key]

    # additional pre-processing of the data should happen here

    df = pd.DataFrame.from_records(filtered_data)
    for field in timestamp_fields:
        df[field] = pd.to_datetime(df[field])

    return df


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    conf = {
        'calls': {
            'data_key': 'tickets',
            'timestamp_fields': (
                'created_at',
                'updated_at'
            ),
            'fields_to_keep': (
                'id',
                'external_id',
                'created_at',
                'updated_at',
                'type',
                'subject',
                'description',
                'priority',
                'status')
        },
        'tickets': {
            'data_key': 'calls',
            'timestamp_fields': (
                'created_at',
                'updated_at'
            ),
            'fields_to_keep': ()
        }
    }

    consumer, data_url = get_args()
    try:
        for data in consumer.read():
            df = clean_to_df(data,
                             conf[consumer.consumer_type()]['data_key'],
                             conf[consumer.consumer_type()]['timestamp_fields'],
                             conf[consumer.consumer_type()]['fields_to_keep'])
            # group by date and hour
            dg = list(df.groupby(by=[df.updated_at.map(lambda d: (d.date(), d.hour))]))
            # write each group to individual s3 prefix
            for key, sdf in dg:
                # sdf['year'] = key[0].year
                # sdf['month'] = key[0].month
                # sdf['day'] = key[0].day
                # sdf['hour'] = key[1]

                data_path = sdf.iloc[0]['updated_at'].strftime(
                    '{}/{}/year=%Y/month=%m/day=%d/hour=%H/%S%M%s.parquet').format(
                    data_url, consumer.consumer_type())

                # check for local execution
                if not data_path.startswith('s3'):
                    os.makedirs(os.path.dirname(data_path), exist_ok=True)

                logging.info('Writing to %s', data_path)
                sdf.to_parquet(data_path, engine='pyarrow', compression=None)

            consumer.checkpoint()
    finally:
        consumer.checkpoint()
