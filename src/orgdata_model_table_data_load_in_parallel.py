import os
import time
import pandas as pd
import concurrent.futures
import argparse
from loguru import logger
from sql_extract.sql_company_query import get_companies_query
from sql_extract.sql_orgdata_model_queries import get_orgdata_model_data_query
from sql_extract.sql_orgdata_model_queries import get_orgdata_model_truncate_query
from sql_extract.sql_orgdata_model_queries import get_orgdata_model_index_rebuild_query
from sql_extract.sql_orgdata_model_queries import get_orgdata_model_insert_json_procedure_query
from sql_conn.sqlserver import SQLServerClient

from sync_functions.model_table_sync import truncate_model_table_data, extract_and_process_model_table_data, \
    insert_model_table_data, rebuild_model_table_index

from dotenv import load_dotenv

load_dotenv()

# adding arguments to the script
parser = argparse.ArgumentParser(
    description="This Python script accepts no input for all companies and -c value for selected company used during testing.")
parser.add_argument("-c", "--company_id", help="company_id value", default=None)  # Required argument
# Parse the arguments
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

# load connector read source
connector_read_source = SQLServerClient(server=source_db_server, port=source_db_port, database=source_db_name
                                        , username=source_db_user, password=source_db_password
                                        , encrypt=source_db_encrypt
                                        , trust_server_certificate=source_db_trust_server_certificate
                                        , timeout_seconds=timeout, autocommit=autocommit)

# load connector write source
connector_write_source = SQLServerClient(server=target_db_server, port=target_db_port, database=target_db_name
                                         , username=target_db_user, password=target_db_password
                                         , encrypt=target_db_encrypt
                                         , trust_server_certificate=target_db_trust_server_certificate
                                         , timeout_seconds=timeout, autocommit=autocommit)

## step 1: load all companies data into a dataframe ##
logger.info(f"Extracting all companies")
companies_query = get_companies_query(input_company_id=input_company_id)
logger.info(f"Companies Query : {companies_query}")
df_companies = connector_read_source.execute_query(companies_query)
df_companies['Company_ID'] = pd.to_numeric(df_companies['Company_ID'], errors='coerce').astype('Int64')
if debug == 1:
    logger.info(df_companies)

# if number_of_companies_to_load and number_of_companies_to_load is not None and number_of_companies_to_load > 0:
if number_of_companies_to_load and number_of_companies_to_load != "" and int(number_of_companies_to_load) > 0:
    logger.info(f"The environment variable 'number_of_companies_to_load' is set to: {number_of_companies_to_load}")
    df_companies = df_companies.head(int(number_of_companies_to_load))
else:
    logger.info("The environment variable 'number_of_companies_to_load' is either not set or has an empty value.")
    df_companies = df_companies

logger.info(f"Number of Companies found is : {df_companies.shape[0]}")

## step 2: truncate the orgdata model table ##
logger.info(f"truncating the orgdata model table")
orgdata_model_truncate_query = get_orgdata_model_truncate_query()
truncate_model_table_data(query=orgdata_model_truncate_query, connector=connector_write_source)

## step 3: load all orgdata model data into a variable
logger.info(f"Extracting and processing orgdata model data for all companies in parallel")


def process_company_orgdata_model_data(input_company_id: int):
    try:
        # extract orgdata model data
        company_id = int(input_company_id)
        logger.info(f"processing company id {company_id}")
        orgdata_model_query = get_orgdata_model_data_query(input_company_id=company_id)
        orgdata_model_insert_json_procedure_query = get_orgdata_model_insert_json_procedure_query()
        df_orgdata_model_data = extract_and_process_model_table_data(company_id=company_id,
                                                                     query=orgdata_model_query,
                                                                     connector=connector_read_source)
        insert_model_table_data(company_id=company_id, query=orgdata_model_insert_json_procedure_query
                                , df=df_orgdata_model_data, connector=connector_write_source, batch_size=batch_size)
        sleep = 0.5
        time.sleep(sleep)
        return company_id, True, len(df_orgdata_model_data)
    except Exception as e:
        logger.exception(f"Error processing company id {input_company_id}: {e}")
        return input_company_id, False, str(e)
    # total_rows = total_rows + len(df_orgdata_model)
    # total_companies = total_companies + 1
    # logger.info(f" total companies processed is : {total_companies} and total rows inserted is : {total_rows}")


failures = []
success_count = 0
total_rows_inserted = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(process_company_orgdata_model_data, int(row['Company_ID'])): int(row['Company_ID']) for
               index, row in df_companies.iterrows()}
    for future in concurrent.futures.as_completed(futures):
        company_id = futures[future]
        try:
            result_company_id, ok, detail = future.result()
        except Exception as e:
            logger.exception(f"Unhandled exception processing company id {company_id}: {e}")
            failures.append(company_id)
        else:
            if ok:
                success_count = success_count + 1
                total_rows_inserted = total_rows_inserted + int(detail)
            else:
                failures.append(company_id)
                logger.error(f"Company {company_id} failed: {detail}")

logger.info(
    f"Parallel run complete: success={success_count}, failures={len(failures)}, total_rows_inserted={total_rows_inserted}")
if failures:
    logger.warning(f"Failed company IDs: {failures}")

## step 4: rebuild the orgdata model table index ##
logger.info(f"rebuilding the orgdata model table index")
orgdata_model_index_rebuild_query = get_orgdata_model_index_rebuild_query()
rebuild_model_table_index(query=orgdata_model_index_rebuild_query, connector=connector_write_source)

logger.info(f"All companies has been processed successfully.")
