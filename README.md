# ETL-Pipeline-Demo
Use Python: Pandas, Boto3, SQLAlchemy, Scikit-learn + MySQL + Snowflake + AWS S3 + Docker in a demo ETL pipeline.

## Installation + Set up
Installation steps assume Python3 is already successfully installed. 

1. Setup and activate virtual environment: `python3 -m venv .venv` then `source .venv/bin/activate`
2. Install the required packages via pip: `pip3 install boto3 pandas numpy sqlalchemy python-dotenv snowflake-connector-python snowflake-sqlalchemy`
3. This project uses AWS. Follow linked instructions below to create 1. an AWS account, 2. an IAM account, and 3. AWS access keys for the IAM account, and then to install the AWS CLI on your machine. You can verify the AWS CLI is set up by running `which aws` in your command prompt to check if an installation filepath is returned. Once this is done, run `aws configure` to add your access keys to the project. NOTE: ensure that IAM account is given sufficient authorization to run commands in the boto3 code (it's probably easiest to give the user 'AdministratorAccess' since this is a test project).
4. This project uses MySQL. Download MySQL Enterprise and MySQL Workbench (linked below) and follow installation instructions. In MySQL Workbench, create a new schema. Then, create a .env file in the root project directory with the fields `MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, and MYSQL_DB` (this information can be found after making the schema in MySQL Workbench).
5. This project uses Snowflake. Create a Snowflake account (you get a free trial!). In Snowflake, run the following to create your database and schema: `CREATE DATABASE <put database name here>;`
`CREATE SCHEMA IF NOT EXISTS <put database name here>.PUBLIC;` Now, in your .env file from step 4, add the following fields (you can find the values from within Snowflake): `SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE`.



## Usage

1. First, generate the data using `python3 generate_data.py` (make sure a 'data' directory has been created)
2. Then, run `s3_load.py` to create a AWS S3 bucket and upload the data to it

## Project outline
* Part 0a: Python (numpy) script to randomly-generate mock data in .csv, .pdf, and .txt formats.
* Part 0b: Python (Boto3) script to establish an AWS S3 cloud storage location, add the randomly-generated data files.
* Part 1: Extract data from S3 (boto3), perform transformations using Pandas
* Part 2a: Load transformed data into a MySQL database via SQLAlchemy
* Part 2b: Load transformed data into Snowflake using snowflake-connector-python
* Part 3a: pull data from both sources back into Pandas via FastAPI call, perform data reconciliation in Python using Pandas and generate results report
* Part 3b: train and run simple prediction model using scikit-learn, generate results report
* Part 4: put reconciled data and results reports back into AWS S3 via Boto3

Containerization: project is containerized using Docker. Once per minute, the program checks if any new files have been added to the AWS s3 storage bucket. If it finds new files to process, the ETL pipeline will automatically re-run.


## Commitments
* No GPT use; rely on official documentation
* Simple, readable code
* Well-tested


## Documentation Sources
* [AWS CLI Installation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) (includes links to creating AWS and IAM accounts + access keys)
* [Boto3 Setup](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html)
* [SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pymysql)
* [MySQL Enterprise](https://www.mysql.com/products/enterprise/)
* [MySQL Workbench](https://www.mysql.com/products/workbench/)