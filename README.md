
# EL Benchmark

Benchmark for EL tools using the TPC-DS benchmark.

## Generate data locally

`cd prepare_mssql_db`

```
python download_data.py
```


## Upload to mssql

we will need to use ODBC driver, install it with the following command:

```
sh install_odbc.sh
```

Then, we can upload the data to mssql with the following command:

```
python upload_to_mssql.py
```


## DLT

`cd dlt`

see results in `/dlt/results`

- connectx_dlt : 0:04:31.809800
- connectx_to_df: 0:04:31.029903
- odbc_dlt     : 0:12:12.047515
- odbc_to_df   : 0:03:01.145907