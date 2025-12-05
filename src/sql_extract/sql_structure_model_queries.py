import re


def get_structure_model_truncate_query() -> str:
    sql_string = f"""
        truncate table thoughtspot.structure_model_tbl_interim;
    """
    return str(sql_string).lower()


def get_structure_model_delete_query(input_company_id: int) -> str:
    sql_string = f"""
		delete from [thoughtspot].[structure_model_tbl_interim] 
		where company_id = {input_company_id}
	"""
    return str(sql_string).lower()


def get_structure_model_index_rebuild_query() -> str:
    sql_string = f"""
        ALTER INDEX [IDX_Structure_model_tbl_CCSI_all] ON [thoughtspot].[structure_model_tbl_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)
    """
    return str(sql_string).lower()


def get_structure_model_data_query(input_company_id: int) -> str:
    sql_string = f"""       
        SELECT a.Company_ID, a.CompanyJob_ID, a.CompanyStructures_ID, a.CompanyStructuresGrades_ID
            , a.CompanyPayMarket_ID, a.CompanyStructuresRangeGroup_ID, a.CompanyStructuresRanges_ID
            , a.Structure_Code, a.Structure_Name, a.Grade_Code, a.Grade_Name, a.RangeGroup_Name, c.PayMarket
            , a.Min * x.CurrencyConversionFactor as Min
            , a.Mid * x.CurrencyConversionFactor as Mid
            , a.Max * x.CurrencyConversionFactor as Max
            , a.IsCurrent, a.IsPublished, a.Rate, a.Currency
            , a.Control_Point, b.RangeType, a.PayType
            , GetDate() AS Loaded_On
            , x.CurrencyConversionFactor as structure_ccf_usd
        FROM dbo.vw_CompanyStructures as a
        join dbo.CompanyStructures_RangeGroup as csrg on a.Company_ID = csrg.Company_ID and a.CompanyStructuresRangeGroup_ID = csrg.CompanyStructuresRangeGroup_ID
        join dbo.CompanyStructures_RangeType as b on csrg.TypeId = b.TypeId
        join dbo.CompanyPayMarkets as c on a.Company_ID = c.Company_ID and a.CompanyPayMarket_ID = c.CompanyPayMarket_ID
        OUTER APPLY [dbo].[fn_tvf_ConvertCurrency_Override](a.Currency, 'USD', getdate(), a.Company_ID) as x
        WHERE a.company_id in ({input_company_id});
        """
    return str(sql_string).lower()


def get_structure_model_insert_json_procedure_query() -> str:
    sql_string = """exec [thoughtspot].[usp_load_structures_model_table_json] @payload=?"""
    return str(sql_string).lower()
