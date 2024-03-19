import connectorx 
from urllib import parse
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime
from connectx_dlt import get_cx_connect_string



if __name__ == "__main__":
    load_dotenv()
    t1 = datetime.now()
    logger.info(f"start time : {t1}")
    conn = get_cx_connect_string()

    query = "SELECT * FROM dbo.UNSW_Flow1"
    df = connectorx.read_sql(conn=conn, query=query)

    # for partition_on it requires a number column -> to apply where num_col > ...
    #! No performance increase with partition_on...
    # df = connectorx.read_sql(conn=conn, query=query, partition_on="dbytes", partition_num=8)

    t2 = datetime.now()
    logger.info(f"end time : {t2}")
    logger.info(f"total time taken : {t2-t1}")
    print(df.head(5))