import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def get_engine():

    # load environment vars from .env
    load_dotenv()

    # build engine url
    url = "snowflake://" + os.getenv("SNOWFLAKE_USER") + ":" + os.getenv("SNOWFLAKE_PASSWORD") + "@" + os.getenv("SNOWFLAKE_ACCOUNT") + "/" + os.getenv("SNOWFLAKE_DATABASE") + "/" + os.getenv("SNOWFLAKE_SCHEMA") + "?" + "warehouse=" + os.getenv("SNOWFLAKE_WAREHOUSE") + "&role=" + os.getenv("SNOWFLAKE_ROLE")

    # create engine
    engine = create_engine(url)

    return engine


def upload_to_snowflake(df, table_name):

    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
    )