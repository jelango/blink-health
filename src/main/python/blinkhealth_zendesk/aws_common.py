import json
from datetime import datetime

import boto3


def writetoS3FromFile(bucketName, srcFileLocation, tgtKeyName):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)

    with open(srcFileLocation, 'rb') as data:
        bucket.upload_fileobj(data, tgtKeyName)


def writeZenDataToS3(bucketName, data, dataKey):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)

    jsondata = json.dumps(data)

    s3key = datetime.now().strftime("yyyy=%Y/mm=%m/dd=%d/hh=%H/key={}/%S%M%s.json").format(dataKey)

    bucket.put_object(Key=s3key, Body=jsondata)
