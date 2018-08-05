#!/usr/bin/env python

import logging

from blinkhealth_zendesk.args import get_args

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    consumer, bucket = get_args()
    df_last = None
    try:
        for df in consumer.read():
            df_curr = df if df_last is None or df_last.empty else df_last.append(df)

            # group by date and hour
            dg = list(df_curr.groupby(by=[df_curr.updated_at.map(lambda d: (d.date(), d.hour))]))
            if len(dg) > 1:
                # write each complete group to individual s3 bucket prefix
                # all but last group are complete, because incremental export data is ordered by timestamp
                for key, sdf in dg[0:-1]:
                    sdf['year'] = sdf['updated_at'].apply(lambda d: d.year)
                    sdf['month'] = sdf['updated_at'].apply(lambda d: d.month)
                    sdf['day'] = sdf['updated_at'].apply(lambda d: d.day)
                    sdf['hour'] = sdf['updated_at'].apply(lambda d: d.hour)
                    del sdf['updated_at']

                    sdf.to_parquet(bucket, engine='fastparquet',
                                   partition_on=['year', 'month', 'day', 'hour'], file_scheme='hive',
                                   compression='UNCOMPRESSED')

            # last group considered to be incomplete
            df_last = dg[-1][1] if dg else None

            consumer.checkpoint([])
    finally:
        consumer.checkpoint([])