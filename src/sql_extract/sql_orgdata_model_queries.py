import re


def get_orgdata_model_truncate_query() -> str:
    sql_string = f"""
        truncate table thoughtspot.orgdata_map_model_tbl_interim;
    """
    return str(sql_string).lower()


def get_orgdata_model_index_rebuild_query() -> str:
    sql_string = f"""
        ALTER INDEX [IDX_orgdata_map_model_tbl_CCSI_all] ON [thoughtspot].[orgdata_map_model_tbl_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)
    """
    return str(sql_string).lower()


def get_orgdata_model_data_query(input_company_id: int) -> str:
    sql_string = f"""
        ;with cte_emp as (
			select ce.Company_ID, ce.companyemployee_id, ce.companypaymarket_id, ce.companyjob_id, ce.currency_code, ce.Rate
			from thoughtspot.vw_employees as ce with (nolock)
			where ce.Company_ID in ({input_company_id})
		), cte_cjpmm as (
					select company_id, CompanyJob_ID, companypaymarket_id 
			from dbo.companyjobspaymarketsmap 
			where company_id in ({input_company_id})
		)
		Select c.company_id, cj.companyjob_id, ce.companyemployee_id, cp.CompanyJobPricing_ID
			, ces.CompanyStructures_ID, ces.CompanyStructuresGrades_ID, ces.CompanyStructuresRangeGroup_ID, ces.CompanyStructuresRanges_ID
			, cpm0.CompanyPayMarket_ID as job_companypaymarket_id, cpm1.companypaymarket_id as emp_companypaymarket_id
			, cpm2.CompanyPayMarket_ID as composite_companypaymarket_id, cpm3.CompanyPayMarket_ID as structures_companypaymarket_id
			, ce.Rate as emp_Rate, cp.Rate as composite_rate, ces.Rate as structures_rate
			, cpm0.Currency_Code as job_curreny_code, ce.Currency_Code as emp_currency_code
			, cp.Currency as composite_currency_code, ces.Currency as structures_currency_code
			, GETDATE() as 'Loaded_On'
		from dbo.CompanyJobs as cj with (nolock) 
		inner join dbo.Company as c with (nolock) 
			on cj.company_id = c.Company_ID 
		left outer join cte_cjpmm as cjpmp with (nolock) 
			on cj.company_id = cjpmp.company_id 
			and cj.companyjob_id = cjpmp.CompanyJob_ID
		left outer join cte_emp as ce with (nolock)
			on cjpmp.Company_ID = ce.Company_ID
			and cjpmp.CompanyJob_ID = ce.CompanyJob_ID
			and cjpmp.CompanyPayMarket_ID = ce.CompanyPayMarket_ID
		left outer join dbo.CompanyJobs_Pricings as cp  with (nolock)
			on cjpmp.Company_ID = cp.Company_ID
			and cjpmp.CompanyJob_ID = cp.CompanyJob_ID
			and cjpmp.CompanyPayMarket_ID = cp.CompanyPayMarket_ID
		left join dbo.vw_CompanyStructures as ces with (nolock)
			on cjpmp.Company_ID = ces.company_id
			and cjpmp.CompanyJob_ID = ces.CompanyJob_ID
			and cjpmp.CompanyPayMarket_ID = ces.CompanyPayMarket_ID
		left join dbo.CompanyStructures_RangeGroup as csrg with (nolock) 
			on ces.Company_ID = csrg.Company_ID 
			and ces.CompanyStructuresRangeGroup_ID = csrg.CompanyStructuresRangeGroup_ID
		left join dbo.CompanyStructures_RangeType as csrt with (nolock) 
			on csrg.TypeId = csrt.TypeId
		left join dbo.CompanyPayMarkets as cpm0 with (nolock)
			on cjpmp.Company_ID = cpm0.Company_ID
			and cjpmp.CompanyPayMarket_ID = cpm0.CompanyPayMarket_ID
		left join dbo.CompanyPayMarkets as cpm1 with (nolock)
			on ce.Company_ID = cpm1.Company_ID
			and ce.CompanyPayMarket_ID = cpm1.CompanyPayMarket_ID
		left join dbo.CompanyPayMarkets as cpm2 with (nolock)
			on cp.Company_ID = cpm2.Company_ID
			and cp.CompanyPayMarket_ID = cpm2.CompanyPayMarket_ID
		left join dbo.CompanyPayMarkets as cpm3 with (nolock)
			on ces.Company_ID = cpm3.Company_ID
			and ces.CompanyPayMarket_ID = cpm3.CompanyPayMarket_ID
		where c.Company_ID in ({input_company_id});
        """
    return str(sql_string).lower()


def get_orgdata_model_insert_json_procedure_query() -> str:
    sql_string = """exec [thoughtspot].[usp_load_orgdata_model_table_json] @payload=?"""
    return str(sql_string).lower()
