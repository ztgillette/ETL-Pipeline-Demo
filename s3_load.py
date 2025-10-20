import boto3
from botocore.exceptions import ClientError
import os

BUCKET_NAME = "etl-demo-bucket1"
REGION = "us-east-1"


# creates (and returns) the s3 bucket that we will be working with
def get_bucket(bucket_name, region):

    s3_client = boto3.client('s3', region_name=region)
    s3 = boto3.resource('s3')


    # we only want 1 bucket for this project
    # ensure that we don't already have a bucket made
    bucket_names = s3_client.list_buckets()["Buckets"]
    #print(bucket_names)
    if len(bucket_names) > 0:
        for bucket in s3.buckets.all():
            return bucket
    
    #assuming no existing buckets, we can create one
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
        )

    except ClientError as e:
        print(e)
        return None
    
    # return the bucket itself, not the collection of 1 buckets
    for bucket in s3.buckets.all():
        return bucket


def upload_to_bucket(bucket, file_name):

    path = "data/" + file_name

    object_name = os.path.basename(path)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(path, bucket, object_name)
    except ClientError as e:
        print(e)



bucket = get_bucket(BUCKET_NAME, REGION)

file_names = os.listdir("data/")
for name in file_names:
    upload_to_bucket(bucket.name, name)