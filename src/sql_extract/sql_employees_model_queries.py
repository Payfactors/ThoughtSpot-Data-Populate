import re


def get_employees_model_truncate_query() -> str:
    sql_string = f"""
        truncate table [thoughtspot].[employee_model_tbl_interim]
    """
    return str(sql_string).lower()


def get_employees_model_index_rebuild_query() -> str:
    sql_string = f"""
        ALTER INDEX [IDX_employee_model_tbl_CCSI_all] ON [thoughtspot].[employee_model_tbl_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)   
    """
    return str(sql_string).lower()


def get_employees_model_data_query(input_company_id: int) -> str:
    sql_string = f"""
        ;with src_emp as (
            select ce.Company_ID, ce.companyemployee_id, ce.companypaymarket_id, ce.companyjob_id
                    , ce.First_Name, ce.Last_Name, ce.Gender, FLOOR(ce.FTE) as FTE, ce.PerformanceRating_PF, ce.Facility
                    , ce.Base, ce.Base_USD, ce.Base_Annualized, ce.Base_Annualized_USD
                    , ce.Bonus, ce.Bonus_USD, ce.LTI, ce.LTI_USD, ce.STI, ce.STI_USD, ce.TCC, ce.TCC_USD, ce.TDC, ce.TDC_USD
                    , ce.Rate, ce.currency_code, ce.City, ce.State, ce.country, ce.Department, ce.DOH
                    , ce.Employee_ID
                    , ce.[UDF_CHAR_1], ce.[UDF_CHAR_2], ce.[UDF_CHAR_3], ce.[UDF_CHAR_4], ce.[UDF_CHAR_5]
                    , ce.[UDF_CHAR_6], ce.[UDF_CHAR_7], ce.[UDF_CHAR_8], ce.[UDF_CHAR_9], ce.[UDF_CHAR_10]
                    , ce.[UDF_CHAR_11], ce.[UDF_CHAR_12], ce.[UDF_CHAR_13], ce.[UDF_CHAR_14], ce.[UDF_CHAR_15]
            from thoughtspot.vw_employees as ce with (nolock)
            where ce.Company_ID in ({input_company_id})
        ), src_emp_mgr as (
            select ce.*
            , cem.Manager_First_Name, cem.Manager_Last_Name, cem.Manager_Job_Code, cem.Manager_Job_Title, cem.Manager_PayMarket
            , cem.Manager_Employee_ID, cem.Manager_Base, cem.Manager_Base_USD , cem.Manager_Base_Annualized
            , cem.Manager_Base_Annualized_USD, cem.Manager_TCC, cem.Manager_TCC_USD, cem.Manager_TDC, cem.Manager_TDC_USD, cem.Manager_Rate
            from src_emp as ce
            inner join thoughtspot.vw_employees_manager as cem with (nolock)
                    on ce.Company_ID = cem.Company_ID
                    and ce.CompanyEmployee_ID = cem.CompanyEmployee_ID
        )
        Select ce.company_id, c.company_name, cj.job_code, cj.job_title, cj.job_family, cj.job_level, cj.jobstatus, cpm.PayMarket
            , ce.Base, ce.Base_USD, ce.Bonus, ce.Bonus_USD, ce.LTI, ce.LTI_USD, ce.STI, ce.STI_USD, ce.Rate as Employee_Rate, ce.currency_code, ce.City, ce.State, ce.country
            , ce.Department, ce.DOH, ce.Employee_ID, ce.First_Name, ce.Gender, ce.Last_Name, ce.FTE, ce.PerformanceRating_PF, cp.rate as pricing_rate
            , cp.base50, ces.Rate as Structure_Rate, ces.[Max], ces.[Mid], ces.[Min], ce.Manager_Employee_ID
            , ce.Manager_Base, ce.Manager_Base_USD, ce.Manager_TCC, ce.Manager_TCC_USD, ce.Manager_TDC, ce.Manager_TDC_USD, ce.Manager_Rate as Manager_Rate
            , ce.companyemployee_id, cpm.companypaymarket_id, cj.companyjob_id
            , Case When DATEDIFF(MONTH,ce.[DOH],getdate()) < 24 then'0-2 years'
                    When DATEDIFF(MONTH,ce.[DOH],getdate()) >= 24 AND DATEDIFF(MONTH,ce.[DOH],getdate()) < 60 then'2-5 years'
                    When DATEDIFF(MONTH,ce.[DOH],getdate()) >= 60 AND DATEDIFF(MONTH,ce.[DOH],getdate()) < 120 then'5-10 years'
                    When DATEDIFF(MONTH,ce.[DOH],getdate()) >= 120 then'more than 10 years'
                    End as 'Tenure_Group'
            , DATEDIFF(YEAR, [DOH], getdate()) As 'Tenure_Years_in_Service'
            , DATEDIFF(MONTH, [DOH], getdate()) As 'Tenure_Months_in_Service'
            , GETDATE() as 'Loaded_On'
            , ce.Base_Annualized, ce.Base_Annualized_USD, ce.Facility
            , ce.Manager_First_Name, ce.Manager_Last_Name, ce.Manager_Job_Code, ce.Manager_Job_Title, ce.Manager_PayMarket
            , ce.UDF_CHAR_1 as emp_udf_1, ce.UDF_CHAR_2 as emp_udf_2, ce.UDF_CHAR_3 as emp_udf_3, ce.UDF_CHAR_4 as emp_udf_4
            , ce.UDF_CHAR_5 as emp_udf_5, ce.UDF_CHAR_6 as emp_udf_6, ce.UDF_CHAR_7 as emp_udf_7, ce.UDF_CHAR_8 as emp_udf_8
            , ce.UDF_CHAR_9 as emp_udf_9, ce.UDF_CHAR_10 as emp_udf_10, ce.UDF_CHAR_11 as emp_udf_11, ce.UDF_CHAR_12 as emp_udf_12
            , ce.UDF_CHAR_13 as emp_udf_13, ce.UDF_CHAR_14 as emp_udf_14, ce.UDF_CHAR_15 as emp_udf_15
            , cj.UDF_CHAR_1 as job_udf_1, cj.UDF_CHAR_2 as job_udf_2, cj.UDF_CHAR_3 as job_udf_3
            , cj.UDF_CHAR_4 as job_udf_4, cj.UDF_CHAR_5 as job_udf_5
            , cou.Country_Name, ce.TCC, ce.TCC_USD, ce.TDC, ce.TDC_USD, ce.Manager_Base_Annualized, ce.Manager_Base_Annualized_USD
            , ces.PayType, cecv.CalculatedValueForRatePayType
        from src_emp_mgr as ce with (nolock)
        join dbo.Company as c with (nolock) 
            on ce.company_id = c.Company_ID 
        left outer join dbo.CompanyJobs as cj with (nolock) 
            on ce.Company_ID = cj.Company_ID
            and ce.CompanyJob_ID = cj.CompanyJob_ID
        left outer join dbo.CompanyJobs_Pricings as cp  with (nolock)
            on ce.Company_ID = cp.Company_ID
            and ce.CompanyJob_ID = cp.CompanyJob_ID
            and ce.CompanyPayMarket_ID = cp.CompanyPayMarket_ID
            and cp.Recency = 1
        left join dbo.CompanyPayMarkets as cpm with (nolock)
            on ce.Company_ID = cpm.Company_ID
            and ce.CompanyPayMarket_ID = cpm.CompanyPayMarket_ID
        left join dbo.vw_employeesstructureinfo as ces with (nolock)
            on ce.Company_ID = ces.company_id
            and ce.CompanyJob_ID = ces.CompanyJob_ID
            and ce.CompanyEmployee_ID = ces.CompanyEmployee_ID
            and ce.CompanyPayMarket_ID = ces.CompanyPayMarket_ID
        left join dbo.Country as cou with (nolock)
            on ce.Country = cou.Country_Code
        Outer Apply [dbo].[fn_tvf_GetCalculatedEmployeeCompValueForRangeGroupByRatePayType](ces.PayType, ces.Rate, ces.Currency, ce.Company_ID, ce.CompanyEmployee_ID) as cecv
        where ce.Company_ID in ({input_company_id}) 
        """
    return str(sql_string).lower()


def get_employees_model_insert_json_procedure_query() -> str:
    sql_string = """
        exec [thoughtspot].[usp_load_employees_model_table_json] @payload=?
    """
    return str(sql_string).lower()
