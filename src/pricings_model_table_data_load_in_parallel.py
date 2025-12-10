import sys
import os
import time
import json
import pandas as pd
import argparse
import concurrent.futures
from loguru import logger

from sql_extract.sql_company_query import get_companies_query
from sql_extract.sql_pricings_model_queries import get_company_pricings_count
from sql_extract.sql_pricings_model_queries import get_pricings_model_data_query
from sql_extract.sql_pricings_model_queries import get_pricings_model_truncate_query
from sql_extract.sql_pricings_model_queries import get_pricings_model_delete_query
from sql_extract.sql_pricings_model_queries import get_pricings_model_index_rebuild_query
from sql_extract.sql_pricings_model_queries import get_pricings_model_insert_interim_table_per_company_query
from sql_extract.sql_pricings_model_queries import get_pricings_model_table_switch_query
from sql_conn.sqlserver import SQLServerClient

from sync_functions.model_table_sync import truncate_model_table_data
from sync_functions.model_table_sync import extract_and_process_model_table_data
from sync_functions.model_table_sync import delete_model_table_data
from sync_functions.model_table_sync import rebuild_model_table_index
from sync_functions.model_table_sync import bulk_insert_dataframe
from sync_functions.model_table_sync import execute_proc_query

from dotenv import load_dotenv

load_dotenv()

# adding arguments to the script
parser = argparse.ArgumentParser(
    description="This Python script accepts no input for all companies and -c value for selected company used during testing.")
parser.add_argument("-c", "--company_id", help="company_id value", default=None)
args = parser.parse_args()
input_company_id = args.company_id
if input_company_id is not None:
    logger.info(f"input company_id value is : {input_company_id}")

# loading env variables
env_name = os.getenv('env_name')
source_db_server = os.getenv('source_db_server')
source_db_port = os.getenv('source_db_port')
source_db_name = os.getenv('source_db_name')
source_db_user = os.getenv('source_db_user')
source_db_password = os.getenv('source_db_password')
source_db_encrypt = os.getenv('source_db_encrypt')
source_db_trust_server_certificate = os.getenv('source_db_trust_server_certificate')
target_db_server = os.getenv('target_db_server')
target_db_port = os.getenv('target_db_port')
target_db_name = os.getenv('target_db_name')
target_db_user = os.getenv('target_db_user')
target_db_password = os.getenv('target_db_password')
target_db_encrypt = os.getenv('target_db_encrypt')
target_db_trust_server_certificate = os.getenv('target_db_trust_server_certificate')
timeout = int(os.getenv('timeout'))
max_workers = int(os.getenv('max_workers'))
autocommit = os.getenv('autocommit')
batch_size = int(os.getenv('batch_size'))
number_of_companies_to_load = os.getenv('number_of_companies_to_load')
debug = int(os.getenv('debug'))
command_timeout = int(os.getenv('command_timeout', 0))  # Default to 0 (wait forever)

# load connector read source
connector_read_source = SQLServerClient(server=source_db_server, port=source_db_port, database=source_db_name
                                        , username=source_db_user, password=source_db_password
                                        , encrypt=source_db_encrypt,
                                        trust_server_certificate=source_db_trust_server_certificate
                                        , timeout_seconds=timeout, autocommit=autocommit
                                        , command_timeout=command_timeout)

# load connector write source
connector_write_source = SQLServerClient(server=target_db_server, port=target_db_port, database=target_db_name
                                         , username=target_db_user, password=target_db_password
                                         , encrypt=target_db_encrypt,
                                         trust_server_certificate=target_db_trust_server_certificate
                                         , timeout_seconds=timeout, autocommit=autocommit
                                         , command_timeout=command_timeout)

# # step 1: load all companies data into a dataframe ##
logger.info("Step 1: Loading companies list")
companies_query = get_companies_query(input_company_id=input_company_id)
df_companies = connector_read_source.execute_query(companies_query)
df_companies['Company_ID'] = pd.to_numeric(df_companies['Company_ID'], errors='coerce').astype('Int64')
if debug == 1:
    logger.info(df_companies)

if number_of_companies_to_load and number_of_companies_to_load != "" and int(number_of_companies_to_load) > 0:
    logger.info(f"The environment variable 'number_of_companies_to_load' is set to: {number_of_companies_to_load}")
    df_companies = df_companies.head(int(number_of_companies_to_load))
else:
    logger.info("The environment variable 'number_of_companies_to_load' is either not set or has an empty value.")

logger.info(f"Number of Companies found is : {df_companies.shape[0]}")

# # step 2: truncate or delete the data for a company in the pricings model table ##
if input_company_id is None:
    logger.info(f"truncating the pricings model table")
    pricings_model_truncate_query = get_pricings_model_truncate_query()
    truncate_model_table_data(query=pricings_model_truncate_query, connector=connector_write_source)
else:
    logger.info(f"deleting the data for company : {input_company_id} in pricings model table")
    delete_query = get_pricings_model_delete_query(input_company_id=int(input_company_id))
    delete_model_table_data(company_id=int(input_company_id), query=delete_query, connector=connector_write_source)


# # step 3: process function definition ##
def process_company_pricings_model_data(company_id: int):
    try:
        company_start_time = time.time()
        logger.info(f"processing company id {company_id}")
        # # load pricings model data into dataframe
        pricings_model_query = get_pricings_model_insert_interim_table_per_company_query(input_company_id=company_id)
        if debug == 1:
            logger.info(f"pricings_model_query is : {pricings_model_query}")
        affected_rows = execute_proc_query(query=pricings_model_query, connector=connector_write_source)
        rows_inserted = int(affected_rows['rows_inserted'][0])
        company_processing_time = time.time() - company_start_time
        logger.info(f"Procedure executed for company {company_id}, affected rows: {rows_inserted}"
                    f", time taken: {company_processing_time:.2f}s")

        sleep = 0.5
        time.sleep(sleep)
        return company_id, True, rows_inserted

    except Exception as e:
        logger.exception(f"Error processing company id {company_id}: {e}")
        return company_id, False, str(e)


# # step 4: PARALLEL PROCESSING - Process all companies in parallel ##
logger.info("Step 3: Starting parallel processing of all companies")

total_companies = len(df_companies)
total_inserted = 0
failures = []
success_count = 0

logger.info("=" * 80)
logger.info(f"Processing {total_companies} companies in parallel with {max_workers} workers")
logger.info("=" * 80)

extraction_start = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {
        executor.submit(process_company_pricings_model_data, int(row['Company_ID'])): int(row['Company_ID'])
        for _, row in df_companies.iterrows()
    }

    for future in concurrent.futures.as_completed(futures):
        company_id = futures[future]
        try:
            result_company_id, ok, result_data = future.result()

            if ok:
                success_count += 1
                if isinstance(result_data, (int, float)) and result_data > 0:
                    total_inserted += int(result_data)
            else:
                failures.append(company_id)
                logger.error(f"Company {company_id} failed: {result_data}")

            time_spent_till_now = time.time() - extraction_start
            rows_loaded_till_now = total_inserted
            companies_processed_till_now = success_count
            logger.info(f"Companies processed status : {companies_processed_till_now}/{total_companies}"
                        f", Rows loaded till now: {rows_loaded_till_now:,}"
                        f", Time spent till now: {time_spent_till_now:.2f}s")
        except Exception as e:
            logger.exception(f"Unhandled exception processing company id {company_id}: {e}")
            failures.append(company_id)

overall_extraction_time = time.time() - extraction_start

# Log processing summary
logger.info("=" * 80)
logger.info("PARALLEL PROCESSING SUMMARY:")
logger.info(f"  Total companies: {total_companies}")
logger.info(f"  Successful: {success_count}")
logger.info(f"  Failed: {len(failures)}")
if failures:
    logger.warning(f"  Failed company IDs: {failures}")
logger.info(f"  Total rows inserted: {total_inserted:,}")
logger.info(f"  Overall time: {overall_extraction_time:.2f}s")
logger.info("=" * 80)

# # step 5: reorganize the pricings model table columnstore index ##
if total_inserted > 0 and input_company_id is None:
    rebuild_start = time.time()
    logger.info(f"rebuilding the pricings model table index")
    pricings_model_index_rebuild_query = get_pricings_model_index_rebuild_query()
    rebuild_model_table_index(query=pricings_model_index_rebuild_query, connector=connector_write_source)
    rebuild_time = time.time() - rebuild_start
    logger.info(f"Index rebuild complete in {rebuild_time:.2f}s")
else:
    logger.info("Skipping index rebuild - no data was inserted or we ran it only for one company")

# # step 6: rename the table, constraint & column store index to main table ##
if input_company_id is None:
    logger.info(f"Interim table needs to be switched with main model table")
    rename_query = get_pricings_model_table_switch_query()
    connector_write_source.execute_non_query(rename_query)
else:
    logger.info(f"All good, no need to rename")

overall_time = time.time() - extraction_start

# Final Summary
logger.info("=" * 80)
logger.info(f"FINAL SUMMARY:")
logger.info(f"  Companies processed: {success_count}/{len(df_companies)}")
logger.info(f"  Failed companies: {len(failures)}")
if failures:
    logger.warning(f"  Failed company IDs: {failures}")
logger.info(f"  Total rows inserted: {total_inserted:,}")
logger.info(f"  Average rows per company: {total_inserted // max(success_count, 1):,}")
logger.info(f"  Overall time: {overall_time:.2f}s")
logger.info(f"All companies have been processed successfully.")
logger.info("=" * 80)