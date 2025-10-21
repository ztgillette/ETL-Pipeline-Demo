from generate_data import create_csv_data
from s3_load import upload_new_files
from etl import etl

"""
One-off demonstration of the pipeline: creates CSV data, uploads the data to AWS S3, then completes the ETL pipeline
"""

# first, generate some new data
num_files_created = create_csv_data()

# then, upload it to our AWS S3 bucket
upload_new_files(num_files_created)

# next, run the etl pipeline
etl()
