import boto3
import io
import pandas as pd
from s3_load import BUCKET_NAME, REGION, get_bucket
from my_sql import upload_to_sql_server
from sf import upload_to_snowflake


def download_from_bucket(bucket_name, file_name):
    """
    Gets data (as pandas df) from AWS S3 bucket

    Args:
        bucket_name (str): the name of the AWS S3 bucket
        file_name (str): the name of the .csv file stored in the bucket

    Returns:
        pandas df: the data 
    """

    s3 = boto3.client('s3')

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    return df


def extract():
    """
    Extracts data from AWS S3 bucket

    Returns:
        list of pandas df objects: a list of all the data being extracted from the bucket
        list (str): the file_names of all the .csv files being extracted
    """


    # Get names of all files in AWS S3
    s3 = boto3.client('s3')
    bucket = get_bucket(BUCKET_NAME, REGION)
    s3_file_name_objects = bucket.objects.all()

    # Determine files to be processed
    to_process = []
    for file_name_obj in s3_file_name_objects:

        file_name = file_name_obj.key
        to_process.append(file_name)

    # Create df of files to process
    df_list = []
    for file_name in to_process:

        df = download_from_bucket(BUCKET_NAME, file_name)
        df_list.append(df)

    return df_list, to_process


def transform(dfs):
    """
    Transforms data so that it is ready to be loaded.

    Args:
        dfs (list of pandas df objects): a list of all the data to be transformed
    
    Returns:
        pandas df object: a combined df of all transformed data
    """

    transformed_dfs = []

    for df in dfs:

        new_df = pd.DataFrame(df)

        # drop all rows with empty/NA values
        new_df = new_df.dropna()

        # make all IDs 6-digit strings
        new_df["ID"] = new_df["ID"].astype(float).astype(int)
        new_df = new_df[(new_df["ID"] >= 1) & (new_df["ID"] <= 999999)]
        new_df["ID"] = new_df["ID"].astype(str)
        new_df["ID"] = new_df["ID"].apply(lambda x: "0"*(6-len(x)) + x)

        # convert year numbers to text
        new_df["Year"] = new_df["Year"].astype(int).astype(str)
        new_df["Year"] = new_df["Year"].apply(lambda x: "Freshman" if x=="1" else "Sophomore" if x=="2" else "Junior" if x=="3" else "Senior" if x=="4" else pd.NA)
        new_df = new_df.dropna()

        # remove data for students with below 0 or above 100
        new_df = new_df[(new_df["Midterm1"] >= 0) & (new_df["Midterm1"] <= 100) & (new_df["Midterm2"] >= 0) & (new_df["Midterm2"] <= 100) & (new_df["Midterm3"] >= 0) & (new_df["Midterm3"] <= 100)]
        
        # round miderm scores to nearest half
        new_df["Midterm1"] = (new_df["Midterm1"] * 2).round() / 2
        new_df["Midterm2"] = (new_df["Midterm2"] * 2).round() / 2
        new_df["Midterm3"] = (new_df["Midterm3"] * 2).round() / 2

        # make final pass/fail binary
        new_df = new_df.rename(columns={"Final Exam Pass?": "Passed"})
        new_df["Passed"] = new_df["Passed"].astype(int).apply(lambda x: x if x in [0, 1] else pd.NA)
        new_df = new_df.dropna()
        new_df["Passed"] = new_df["Passed"].astype(bool)

        # finally, append new df to the list to return
        transformed_dfs.append(new_df)

    # Finally, join all data to exclude non-unique IDs
    concat_df = pd.concat(transformed_dfs)
    unique_id_df = concat_df.drop_duplicates(subset=["ID"])
    unique_id_df = unique_id_df.sort_values(by="ID")

    return unique_id_df


def etl():
    """
    Calls each component of the ETL pipeline
    """

    # 1: E: pull bucket contents from AWS S3 storage as pandas df
    dfs, _ = extract()

    # 2: T: transform data
    transformed_df = transform(dfs)

    # 3: L: Load data into MySQL server and Snowflake
    # MySQL
    upload_to_sql_server(transformed_df, "MidtermGrades")
    upload_to_snowflake(transformed_df, "MidtermGrades")
