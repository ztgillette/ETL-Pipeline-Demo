import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def get_engine():
    """
    Creates and returns an engine that can be used to interact with Snowflake from within Python.

    Returns:
        engine object: the engine object
    """

    # load environment vars from .env
    load_dotenv()

    # build engine url
    url = "snowflake://" + os.getenv("SNOWFLAKE_USER") + ":" + os.getenv("SNOWFLAKE_PASSWORD") + "@" + os.getenv("SNOWFLAKE_ACCOUNT") + "/" + os.getenv("SNOWFLAKE_DATABASE") + "/" + os.getenv("SNOWFLAKE_SCHEMA") + "?" + "warehouse=" + os.getenv("SNOWFLAKE_WAREHOUSE") + "&role=" + os.getenv("SNOWFLAKE_ROLE")

    # create engine
    engine = create_engine(url)

    return engine


def upload_to_snowflake(df, table_name) -> None:
    """
    Adds data from a pandas df to a given table in Snowflake.

    Args:
        df (pandas df object): the dataframe whose data will be added
        table_name (str): the name of the Snowflake table in which to add the data
    """

    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
    )