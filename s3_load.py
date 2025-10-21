import boto3
from botocore.exceptions import ClientError
import os

BUCKET_NAME = "etl-demo-bucket1"
REGION = "us-east-1"


def get_bucket(bucket_name, region):
    """
    Creates (if first run) or returns existing (second+ run) AWS S3 bucket that is the starting point for data in the ETL pipeline

    Args:
        bucket_name (str): the name of the bucket
        region (str): the region used for our AWS operations (ideally us-east-1 in Boston)

    Returns:
        boto3 bucket object
    """

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
    """
    Uploads a .csv file to the AWS S3 bucket

    Args:
        bucket (boto3 bucket object): the bucket
        file_name (str): the name of the file we want to add
    """

    path = "data/" + file_name

    object_name = os.path.basename(path)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(path, bucket, object_name)
    except ClientError as e:
        print(e)


def upload_new_files(num_new_files):
    """
    Uploads new .csv files to AWS S3 bucket

    Args:
        num_new_files (int): the number of new files to be uploaded; used to determine which files in 
        /data should be uploaded based on the number in their file name
    """

    bucket = get_bucket(BUCKET_NAME, REGION)
    
    for i in range(num_new_files):

        file_name_num = len(os.listdir("data/")) - num_new_files + 1 + i
        file_name = "csv_data_" + str(file_name_num) + ".csv"
    
        upload_to_bucket(bucket.name, file_name)