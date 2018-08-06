### Pre-requisites

Python `virtualenv-3.x` binary should be available on the path.


### Setup

```bash
./prebuild.sh
```

Running export to S3:

```bash
zendesk_to_s3.py calls -a support@blinkhealth.com -t 06is94FOOrtqmf3VVkP2WOPz40y94LbVEUiC1nDF -b s3://blink-dw-data-zendesk/calls
zendesk_to_s3.py tickets -a support@blinkhealth.com -t 06is94FOOrtqmf3VVkP2WOPz40y94LbVEUiC1nDF -b s3://blink-dw-data-zendesk/tickets
```

Create AWS Athena table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS zendesk_export (
         id int,
         updated_at timestamp 
) PARTITIONED BY(
         year string,
         month string,
         day string,
         hour string,
         key string 
) STORED AS PARQUET LOCATION 's3://blink-dw-data-zendesk/';
```

After new export operation, update metadata:
```sql
MSCK REPAIR TABLE zendesk_export;
```

Get distribution of tickets, calls by days:
```sql
SELECT key, DATE(updated_at) as day, count 
FROM zendesk_export
GROUP BY key, DATE(updated_at)
```
