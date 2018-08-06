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
                    # sdf['year'] = key[0].year
                    # sdf['month'] = key[0].month
                    # sdf['day'] = key[0].day
                    # sdf['hour'] = key[1]

                    s3_path = sdf.iloc[0]['updated_at'].strftime(
                        '{}/year=%Y/month=%m/day=%d/hour=%H/key={}/%S%M%s.parquet').format(
                        bucket, consumer.consumer_type())
                    sdf.to_parquet(s3_path, engine='pyarrow', compression=None)

            # last group considered to be incomplete
            df_last = dg[-1][1] if dg else None

            consumer.checkpoint([])
    finally:
        consumer.checkpoint([])
