import sys
import os
import time
import json
import pandas as pd
import argparse
import concurrent.futures
import json
import random
import time
import numpy as np  # local import to handle NaN/Inf

from loguru import logger

from sql_conn.sqlserver import SQLServerClient  # noqa: F401

from dotenv import load_dotenv

load_dotenv()
debug = int(os.getenv('debug'))


def truncate_model_table_data(query: str, connector: SQLServerClient):
    if debug == 1:
        logger.info(f"model_table_truncate_query is : {query}")
    connector.execute_non_query(query)
    logger.info(f"job model truncate query executed successfully")


def delete_model_table_data(company_id: int, query: str, connector: SQLServerClient):
    if debug == 1:
        logger.info(f"model_table_delete_query is : {query}")
    connector.execute_non_query(query)
    logger.info(f"job model delete query for company id : {company_id} executed successfully")


def extract_and_process_model_table_data(company_id: int, query: str, connector: SQLServerClient):
    # load structures model query
    if debug == 1:
        logger.info(f"model_table_query is : {query}")
    df_model_table_data = connector.execute_query(query)
    logger.info(f"data found for company id {company_id} is : {df_model_table_data.shape[0]}")
    for column in df_model_table_data.columns:
        if column.upper().endswith('_ID'):
            df_model_table_data[column] = pd.to_numeric(df_model_table_data[column], errors='coerce').astype('Int64')
            # if debug == 1:
            #   logger.info(f"Converted column '{column}' to int64")
    return df_model_table_data


def insert_model_table_data(company_id: int, query: str, df: pd.DataFrame, connector: SQLServerClient,
                            batch_size: int = 1000, sleep_min: float = 0.05, sleep_max: float = 0.50):
    total_rows = len(df)
    logger.info(f"total rows for company id {company_id} to insert is : {total_rows}")
    batch_number = 0
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i + batch_size].copy()
        # Ensure JSON-safe nulls: convert to object dtype first so None is preserved & convert to json
        batch_df = batch_df.astype(object)
        try:
            import numpy as np  # local import to handle NaN/Inf
            batch_df = batch_df.replace({pd.NA: None, np.nan: None, np.inf: None, -np.inf: None})
        except Exception:
            batch_df = batch_df.where(pd.notna(batch_df), None)
        payload = batch_df.to_dict(orient='records')
        json_param = (json.dumps(payload, default=str, allow_nan=False),)
        # if debug == 1:
        #     logger.info(f"current batch is : {json_param}")
        connector.execute_non_query(query, json_param)
        batch_number = batch_number + 1
        # logger.info(f"batch {batch_number} for company id {company_id} with {len(payload)} rows inserted")
        # Random jitter between batches to reduce lock contention/deadlocks
        if sleep_min and sleep_max and sleep_max > 0:
            time.sleep(random.uniform(sleep_min, sleep_max))


def insert_model_table_data_fast(company_id: int, table_name: str, df: pd.DataFrame
                                 , connector: SQLServerClient, batch_size: int = 100000
                                 , sleep_min: float = 0.05, sleep_max: float = 0.50):
    """
    Fast bulk insert using pyodbc's fast_executemany instead of JSON parameters.
    No server filesystem access needed - streams from client.
    """
    total_rows = len(df)
    logger.info(f"Company {company_id}: Inserting {total_rows} rows using fast_executemany")

    if total_rows == 0:
        logger.info(f"Company {company_id}: No rows to insert")
        return
    # Build parameterized INSERT statement
    columns = df.columns.tolist()
    placeholders = ','.join(['?' for _ in columns])
    column_names = ','.join([f'[{col}]' for col in columns])

    insert_sql = f"""
        INSERT INTO {table_name} WITH (TABLOCK) 
        ({column_names}) 
        VALUES ({placeholders})
    """
    # Process in batches to manage memory
    batch_number = 0
    for i in range(0, total_rows, batch_size):
        batch_start = time.time()
        batch_df = df.iloc[i:i + batch_size].copy()

        # Convert DataFrame to list of tuples for executemany
        # pyodbc handles None/NULL automatically
        batch_df = batch_df.astype(object).where(pd.notna(batch_df), None)
        param_rows = [tuple(row) for row in batch_df.itertuples(index=False, name=None)]

        # Use fast_executemany for bulk insert
        rows_inserted = connector.executemany(insert_sql, param_rows)
        batch_number += 1
        batch_time = time.time() - batch_start
        logger.info(
            f"Company {company_id}: Batch {batch_number} : ({len(param_rows)} rows) inserted in {batch_time:.2f}s"
        )
    logger.info(f"Company {company_id}: Total {total_rows} rows inserted in {batch_number} batches")
    if sleep_min and sleep_max and sleep_max > 0:
        time.sleep(random.uniform(sleep_min, sleep_max))


def rebuild_model_table_index(query: str, connector: SQLServerClient):
    if debug == 1:
        logger.info(f"model_table_index_rebuild_query is : {query}")
    connector.execute_non_query(query)
    logger.info(f"job model index rebuild query executed successfully")


# def execute_proc_query(query: str, connector: SQLServerClient):
#     if debug == 1:
#         logger.info(f"model_table_execute_proc_query is : {query}")
#     # connector.execute_non_query(query)
#     df_model_table_data = connector.execute_query(query)
#     # logger.info(f"data found for company id {company_id} is : {df_model_table_data.shape[0]}")
#     logger.info(df_model_table_data)
#     for column in df_model_table_data.columns:
#         if column.upper().endswith('_ID'):
#             df_model_table_data[column] = pd.to_numeric(df_model_table_data[column], errors='coerce').astype('Int64')
#             # if debug == 1:
#             #   logger.info(f"Converted column '{column}' to int64")
#     return df_model_table_data


# In sync_functions/model_table_sync.py
def execute_proc_query(query: str, connector: SQLServerClient):
    if debug == 1:
        logger.info(f"model_table_execute_proc_query is : {query}")
    
    # Use the new method for stored procedures that may have multiple result sets
    result = connector.execute_stored_procedure(query)
    logger.info(f"Procedure result: {result}")
    if isinstance(result, pd.DataFrame):
        for column in result.columns:
            if column.upper().endswith('_ID'):
                result[column] = pd.to_numeric(result[column], errors='coerce').astype('Int64')
        return result
    else:
        # result is just rows affected count
        return result


def bulk_insert_dataframe(df: pd.DataFrame, table_name: str, connector: SQLServerClient,
                          batch_size: int = 102400):
    """
    Bulk insert DataFrame using fast_executemany with TABLOCK for minimal logging.
   Args:
        df: DataFrame to insert
        table_name: Target table name (e.g., "[schema].[table]")
        connector: SQLServerClient instance
        batch_size: Rows per batch (default 250k for optimal columnstore performance)
    """
    if len(df) == 0:
        logger.warning("Empty DataFrame, nothing to insert")
        return

    logger.info(f"Bulk inserting {len(df)} rows into {table_name}")

    # Build INSERT statement
    columns = df.columns.tolist()
    column_names = ','.join([f'[{col}]' for col in columns])
    placeholders = ','.join(['?' for _ in columns])

    insert_sql = f"""
        INSERT INTO {table_name} WITH (TABLOCK)
        ({column_names})
        VALUES ({placeholders})
    """

    # Insert in batches
    total_rows = len(df)
    batch_number = 0
    start_time = time.time()

    for i in range(0, total_rows, batch_size):
        batch_start = time.time()
        batch_df = df.iloc[i:i + batch_size].copy()
        # batch_df = batch_df.astype(object).where(pd.notna(batch_df), None)
        batch_df = batch_df.astype(object)
        try:
            batch_df = batch_df.replace({pd.NA: None, np.nan: None, np.inf: None, -np.inf: None})
        except Exception:
            batch_df = batch_df.where(pd.notna(batch_df), None)
        param_rows = [tuple(row) for row in batch_df.itertuples(index=False, name=None)]

        connector.executemany(insert_sql, param_rows)
        batch_number += 1
        batch_time = time.time() - batch_start

        logger.info(
            f"Batch {batch_number}: {len(param_rows)} rows inserted in {batch_time:.2f}s "
            f"(progress: {min(i + batch_size, total_rows)}/{total_rows})"
        )

    total_time = time.time() - start_time
    logger.info(f"Bulk insert complete: {total_rows} rows in {batch_number} batches, {total_time:.2f}s total")