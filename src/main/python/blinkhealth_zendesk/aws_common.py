from urllib.parse import urlparse
from botocore.exceptions import ClientError

import json
import boto3


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
