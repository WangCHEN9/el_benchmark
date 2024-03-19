from typing import Iterator, Any

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from sql_database import sql_database, sql_table, Table


def load_select_tables_from_database() -> None:

    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam", destination="duckdb", dataset_name="rfam_data", full_refresh=False,
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source = sql_database(credentials).with_resources("genome")
    source.genome.apply_hints(incremental=dlt.sources.incremental("created"))

    info = pipeline.run(source, write_disposition="append")   # write_disposition="replace"
    print(info)


def reflect_and_connector_x() -> None:
    """Uses sql_database to reflect the table schema and then connectorx to load it. Connectorx has rudimentary type support ie.
    is not able to use decimal types and is not providing length information for text and binary types.

    NOTE: mind that for DECIMAL/NUMERIC the data is converted into float64 and then back into decimal Python type. Do not use it
    when decimal representation is important ie. when you process currency.
    """

    # uncomment line below to get load_id into your data (slows pyarrow loading down)
    dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True

    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam_cx", destination='duckdb', dataset_name="rfam_data_cx"
    )

    # Credentials for the sample database.
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )

    # below we reflect family and genome tables. detect_precision_hints is set to True to emit
    # full metadata
    sql_alchemy_source = sql_database(
        credentials, detect_precision_hints=True
    ).with_resources("genome")

    # display metadata
    print(sql_alchemy_source.genome.columns)

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
            protocol="binary",
        )

    # Use columns from sql_alchemy source to define columns for connectorx
    genome_cx = read_sql_x(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        "SELECT * FROM genome LIMIT 100",
    ).with_name("genome")

    # Use columns from sql_alchemy source to define columns for connectorx
    genome_cx.apply_hints(columns=sql_alchemy_source.genome.columns)

    info = pipeline.run([genome_cx])
    print(info)
    print(pipeline.default_schema.to_pretty_yaml())



if __name__ == "__main__":
    import warnings

    warnings.filterwarnings("ignore")

    load_select_tables_from_database()
    # reflect_and_connector_x()

