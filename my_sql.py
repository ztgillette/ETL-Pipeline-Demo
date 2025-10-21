from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

def get_engine():

    # load environment vars from .env
    load_dotenv()

    # build engine url
    url = "mysql+pymysql://" + os.getenv("MYSQL_USER") + ":" + os.getenv("MYSQL_PASSWORD") + "@" + os.getenv("MYSQL_HOST") + "/" + os.getenv("MYSQL_DB")

    # create engine
    engine = create_engine(url)

    return engine

def upload_to_sql_server(df, table_name):

    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
    )
