import re


def get_paymarketsmap_model_truncate_query() -> str:
    sql_string = f"""
        truncate table thoughtspot.paymarketsmap_tbl_interim;
    """
    return str(sql_string).lower()


def get_paymarketsmap_model_index_rebuild_query() -> str:
    sql_string = f"""
        ALTER INDEX [IDX_paymarketsmap_tbl_CCSI_all] ON [thoughtspot].[paymarketsmap_tbl_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)
    """
    return str(sql_string).lower()


def get_paymarketsmap_model_data_query(input_company_id: int) -> str:
    sql_string = f"""       
        Select a.Company_id, a.CompanyJob_ID, a.CompanyPayMarket_ID, cp.PayMarket, getdate() as Loaded_On
        from dbo.companyjobspaymarketsmap as a
        join dbo.CompanyPayMarkets as cp on a.CompanyPayMarket_ID = cp.CompanyPayMarket_ID
        where a.company_id in ({input_company_id});
    """
    return str(sql_string).lower()


def get_paymarketsmap_model_insert_json_procedure_query() -> str:
    sql_string = """exec [thoughtspot].[usp_load_paymarketsmap_model_table_json] @payload=?"""
    return str(sql_string).lower()
