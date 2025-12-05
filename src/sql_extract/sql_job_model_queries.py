import re

def get_job_model_truncate_query() -> str:
    sql_string = f"""
		truncate table [thoughtspot].[jobs_model_tbl_interim]
	"""
    return str(sql_string).lower()


def get_job_model_delete_query(input_company_id: int) -> str:
    sql_string = f"""
		delete from [thoughtspot].[jobs_model_tbl_interim] 
		where company_id = {input_company_id}
	"""
    return str(sql_string).lower()

def get_job_model_index_rebuild_query() -> str:
    sql_string = f"""
		ALTER INDEX [IDX_jobs_model_tbl_CCSI_all] ON [thoughtspot].[jobs_model_tbl_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)
	"""
    return str(sql_string).lower()


def get_job_model_data_query(input_company_id: int) -> str:
    sql_string = f"""       
        ;with CTE_companyjobspaymarketsmap as
		(
			select company_id, CompanyJob_ID, companypaymarket_id 
			from dbo.companyjobspaymarketsmap 
			where company_id in ({input_company_id})
		)
		Select distinct cj.Company_ID, cj.Job_Code, cj.Job_Title, cj.Job_Family, cpm.Paymarket	
		--, ce.Gender
		, cp.Recency as most_recent 
		, cp.effective_date, cp.Base10, cp.Base25, cp.Base50, cp.Base75, cp.Base90, cp.BaseMRP, cp.Rate as Pricing_Rate
		, cjs.Rate as Structure_rate, cjs.Mid, cjs.Min, cjs.Max
		, cast(cjpmp.CompanyPayMarket_ID as int) as CompanyPayMarket_ID	
		--, NULL as CompanyPayMarket_ID
		--, ce.CompanyEmployee_ID
		, Cj.CompanyJob_ID
		, GetDate() AS Loaded_On
		, cj.JobStatus
		from dbo.CompanyJobs as cj with (nolock)
		--left outer join dbo.companyemployees as ce with (nolock)
		--	on cj.company_id = ce.company_id 
		--	and cj.companyjob_id = ce.companyjob_id
		left outer join CTE_companyjobspaymarketsmap as cjpmp with (nolock) 
			on cj.company_id = cjpmp.company_id 
			and cj.companyjob_id = cjpmp.CompanyJob_ID
		left outer join dbo.CompanyPayMarkets as cpm with (nolock)
			on cjpmp.Company_ID = cpm.Company_ID
			and cjpmp.companypaymarket_id = cpm.CompanyPayMarket_ID
		left outer join dbo.CompanyJobs_Pricings as cp with (nolock)
			on cj.Company_ID = cp.Company_ID
			and cj.CompanyJob_ID = cp.CompanyJob_ID 
			and cjpmp.companypaymarket_id = cp.CompanyPayMarket_ID
			and cjpmp.CompanyPayMarket_ID = cp.CompanyPayMarket_ID
		left outer join dbo.vw_companyjobsstructureInfo as cjs with (nolock)
			on cj.company_id = cjs.company_id 
			and cj.CompanyJob_ID = cjs.CompanyJob_ID 
			and cjpmp.companypaymarket_id = cjs.CompanyPayMarket_ID
		where cj.Company_ID in ({input_company_id})
		order by cj.Company_ID
        """
    return str(sql_string).lower()


def get_job_model_insert_json_procedure_query() -> str:
    sql_string = """
		exec [thoughtspot].[usp_load_jobs_model_table_json] @payload=?
		"""
    return str(sql_string).lower()
