from typing import Iterator, Any
import urllib

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


def dlt_el_from_database(
        schema_name:str, 
        table_name:str, 
        cursor_field:str = None,
        chunk_size:int = 1000,
    ) -> None:
    engine = get_mssql_engine()
    source_table:DltResource = sql_database(
        credentials=engine,
        schema=schema_name, 
        table_names=[table_name], 
        defer_table_reflect=False,
        chunk_size=chunk_size,
    )
    if cursor_field:
        source_table.apply_hints(incremental=dlt.sources.incremental(cursor_field))

    pipeline_name = get_pipeline_name(prefix="odbc_", schema_name=schema_name, table_name=table_name)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, 
        destination="filesystem", 
        dataset_name=table_name, 
        full_refresh=False,
        pipelines_dir="el_pipelines",
        progress="log",
    )

    info = pipeline.run(
        source_table, 
        write_disposition="append", # write_disposition="replace"
        loader_file_format="parquet",
    )
    print(info)


if __name__ == "__main__":
    load_dotenv()
    init_logger()
    t1 = datetime.now()
    logger.info(f"start time : {t1}")
    dlt_el_from_database(schema_name="dbo", table_name="UNSW_Flow1", chunk_size=10_000)
    t2 = datetime.now()
    logger.info(f"end time : {t1}")
    logger.info(f"total time taken : {t2-t1}")