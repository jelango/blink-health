from urllib.parse import urlparse
from botocore.exceptions import ClientError

import json
import boto3

streamName = 'zen-datastream'
kinesis_client = boto3.client('kinesis', region_name='us-west-2')

def write_to_s3(url, data):
    parts = urlparse(url)
    bucket = parts.netloc
    key = parts.path.lstrip('/')

    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=json.dumps(data))


def read_from_s3(url, defaults):
    parts = urlparse(url)
    bucket = parts.netloc
    key = parts.path.lstrip('/')

    s3 = boto3.resource('s3')
    try:
        return json.loads(s3.Object(bucket, key).get()['Body'].read().decode("utf-8"))
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return defaults
        else:
            raise ex


def put_to_stream(partid, property_value):
    put_response = kinesis_client.put_record(
                        StreamName=streamName,
                        Data=json.dumps(property_value),
                        PartitionKey=partid)
