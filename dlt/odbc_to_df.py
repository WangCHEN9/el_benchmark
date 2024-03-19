import connectorx 
from urllib import parse
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
from sql_database.utils import get_mssql_engine



if __name__ == "__main__":
    load_dotenv()
    t1 = datetime.now()
    logger.info(f"start time : {t1}")
    conn = get_mssql_engine(if_return_connect_string=True)

    query = "SELECT * FROM dbo.UNSW_Flow1"
    df = pd.read_sql(con=conn, sql=query)

    t2 = datetime.now()
    logger.info(f"end time : {t2}")
    logger.info(f"total time taken : {t2-t1}")
    print(df.head(5))