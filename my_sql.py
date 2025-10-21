from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

def get_engine():
    """
    Creates and returns an engine that can be used to interact with MySQL from within Python.

    Returns:
        engine object: the engine object
    """

    # load environment vars from .env
    load_dotenv()

    # build engine url
    url = "mysql+pymysql://" + os.getenv("MYSQL_USER") + ":" + os.getenv("MYSQL_PASSWORD") + "@" + os.getenv("MYSQL_HOST") + "/" + os.getenv("MYSQL_DB")

    # create engine
    engine = create_engine(url)

    return engine

def upload_to_sql_server(df, table_name):
    """
    Adds data from a pandas df to a given table in MySQL.

    Args:
        df (pandas df object): the dataframe whose data will be added
        table_name (str): the name of the MySQL table in which to add the data
    """

    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
    )
