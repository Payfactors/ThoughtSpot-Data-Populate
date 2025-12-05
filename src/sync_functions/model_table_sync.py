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


def rebuild_model_table_index(query: str, connector: SQLServerClient):
    if debug == 1:
        logger.info(f"model_table_index_rebuild_query is : {query}")
    connector.execute_non_query(query)
    logger.info(f"job model index rebuild query executed successfully")

