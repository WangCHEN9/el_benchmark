from typing import Iterator, Any

import dlt
from dlt.sources import DltResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from sql_database import sql_database, sql_table, Table
from dlt.sources.credentials import ConnectionStringCredentials
from dotenv import load_dotenv
import os
import sys

from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime
from sql_database.utils import get_pipeline_name, get_mssql_engine, init_logger
from urllib import parse


def get_cx_connect_string() -> str:
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    server = 'el-fmk-poc.database.windows.net'
    database = os.getenv('DATABASE')
    trusted_conn = 'no' # or yes

    password = parse.quote_plus(password)

    mssql_url = f'mssql://{username}:{password}@{server}/{database}?encrypt=true&trusted_connection={trusted_conn}'
    return mssql_url


def reflect_and_connector_x(        
        schema_name:str, 
        table_name:str, 
        cursor_field:str = None,
        chunk_size:int = 1000,
    ) -> None:
    """Uses sql_database to reflect the table schema and then connectorx to load it. Connectorx has rudimentary type support ie.
    is not able to use decimal types and is not providing length information for text and binary types.

    NOTE: mind that for DECIMAL/NUMERIC the data is converted into float64 and then back into decimal Python type. Do not use it
    when decimal representation is important ie. when you process currency.
    """

    # uncomment line below to get load_id into your data (slows pyarrow loading down)
    # dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True

    pipeline_name = get_pipeline_name(schema_name=schema_name, table_name=table_name, prefix='cx_')
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, 
        destination="filesystem", 
        dataset_name=table_name, 
        full_refresh=False,
        pipelines_dir="el_pipelines",
        progress="log",
    )

    # define a resource that will be used to read data from connectorx
    @dlt.resource
    def read_sql_x(
        conn_str: str,
        query: str,
    ) -> Iterator[Any]:
        import connectorx as cx  # type: ignore

        yield cx.read_sql(
            conn_str,
            query,
            return_type="arrow2",
        )

    # Use columns from sql_alchemy source to define columns for connectorx
    cx = read_sql_x(
        conn_str= get_cx_connect_string(),
        query = f"SELECT * FROM {schema_name}.{table_name}",
    ).with_name(table_name)

    #* Use columns from sql_alchemy source to define columns for connectorx
    #? Still works without hints?
    # cx.apply_hints(columns=source_table.columns)

    info = pipeline.run(
        [cx], 
        write_disposition="append",
        loader_file_format="parquet",
    )   
    print(info)




if __name__ == "__main__":

    load_dotenv()
    init_logger()
    t1 = datetime.now()
    logger.info(f"start time : {t1}")
    reflect_and_connector_x(schema_name="dbo", table_name="UNSW_Flow1")
    t2 = datetime.now()
    logger.info(f"end time : {t1}")
    logger.info(f"total time taken : {t2-t1}")

