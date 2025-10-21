import boto3
from s3_load import get_bucket, upload_to_bucket, BUCKET_NAME, REGION
import os
import time
from etl import etl

def main():

    # count number of files in AWS S3 storage
    s3 = boto3.client('s3')
    bucket = get_bucket(BUCKET_NAME, REGION)
    s3_file_name_objects = bucket.objects.all()
    num_s3_files = 0
    for file_name in s3_file_name_objects:
        num_s3_files += 1

    # count number of files in data folder
    num_local_files = len(os.listdir("data/"))

    # if S3 storage is missing files, upload them and rerun the ETL pipeline
    if num_local_files > num_s3_files:

        difference = num_local_files - num_s3_files
        print(str(difference) + " new files to process.")

        for i in range(difference):
            file_name = "csv_data_" + str(num_s3_files + 1 + i) + ".csv"
            upload_to_bucket(bucket.name, file_name)

        # wait for upload to complete
        num_s3_files = 0
        while not num_s3_files == num_local_files:

            num_s3_files = 0
            for file_name in bucket.objects.all():
                num_s3_files += 1

            print("#local files", num_local_files)
            print("#s3 files", num_s3_files)
            time.sleep(3)

        # rerun etl pipeline
        etl()

    else:
        print("Nothing to do.")





main()