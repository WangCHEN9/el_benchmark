
from typing import Iterator, Any
import urllib

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import os

from pathlib import Path
from loguru import logger



def init_logger() -> None:
    log_folder_path = Path(os.getenv(f"DBT_LOG_PATH", "./logs"))
    log_file_path = log_folder_path.joinpath(r"el-fmk.log")
    logger.info(f"adding log file path : {log_file_path}")
    logger.add(log_file_path)


def get_mssql_engine(if_return_connect_string:bool=False) -> Engine:
    SERVER = 'tcp:el-fmk-poc.database.windows.net,1433'
    DATABASE = os.getenv('DATABASE')
    USERNAME = os.getenv('USERNAME')
    PASSWORD = os.getenv('PASSWORD')

    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

    quoted = urllib.parse.quote_plus(connectionString)
    if if_return_connect_string:
        return 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(
        'mssql+pyodbc:///?odbc_connect={}'.format(quoted),
        connect_args={"timeout": 60},
    )
    logger.info(f"engine created")
    return engine


def get_pipeline_name(schema_name:str, table_name:str, prefix:str = "", suffix:str ="") -> str:
    return f"{prefix}{schema_name}_{table_name}{suffix}"
