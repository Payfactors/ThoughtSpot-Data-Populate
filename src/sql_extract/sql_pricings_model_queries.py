import re


def get_pricings_model_truncate_query() -> str:
    sql_string = f"""
		truncate table [thoughtspot].[pricing_model_table_interim]
	"""
    return str(sql_string).lower()


def get_pricings_model_delete_query(input_company_id: int) -> str:
    sql_string = f"""
		delete from [thoughtspot].[pricing_model_table_interim] 
		where company_id = {input_company_id}
	"""
    return str(sql_string).lower()


def get_pricings_model_index_rebuild_query() -> str:
    sql_string = f"""
		ALTER INDEX [IDX_pricing_model_table_CCSI_all] ON [thoughtspot].[pricing_model_table_interim] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = COLUMNSTORE)
	"""
    return str(sql_string).lower()


def get_company_pricings_count(input_company_id: int) -> str:
    sql_string = f"""
		Select count(1) as pricings_count 
		from dbo.CompanyJobs_Pricings with (nolock)
		where 1=1
		and company_id = {input_company_id}
		and effective_date >= dateadd(year, -3, getdate())
	"""
    return str(sql_string).lower()


def get_pricings_model_data_query(input_company_id: int, input_years_to_load: int = 3) -> str:
    sql_string = f"""       
            ;with cte_cc as(
                select distinct {input_company_id} as company_id
                            , cc.currency_code
                            , x.currencyconversionfactor
                from dbo.countrycurrency as cc
                cross apply dbo.fn_tvf_convertcurrency_override(currency_code, 'usd', getdate(), {input_company_id}) x
            ), cte_ppm as (
                select cjp.company_id, cjp.companyjob_id, cjp.companypaymarket_id
                    , cjp.companyjobpricing_id as companyjobpricing_id, cjpm.companyjobpricingmatch_id as companyjobpricingmatch_id
                    , cjpm.survey_id, cjpm.survey_job_id, cjpm.survey_data_id
                    , cjpm.exchangedatacut_id as matches_exchangedatacut_id, cjpm.slotted_companyjob_id as matches_slotted_companyjob_id
                    from companyjobs_pricings cjp with (nolock) 
                left outer join companyjobs_pricingsmatches cjpm with (nolock) 
                on cjpm.companyjobpricing_id = cjp.companyjobpricing_id 
                    and cjpm.company_id = cjp.company_id 
                where 1=1
                and cjp.company_id = {input_company_id}
                and cjp.effective_date > dateadd(year, -{input_years_to_load}, getdate())
            )
            select  cjp.company_id,
                    --id columns 
                    cjp.companyjob_id,cp.companypaymarket_id as composite_companypaymarket_id,cjp.companyjobpricing_id as companyjobpricing_id,cjpm.companyjobpricingmatch_id as companyjobpricingmatch_id,
                    s.survey_id,sd.survey_data_id,sj.survey_job_id,cjpm.exchangedatacut_id as matches_exchangedatacut_id,cjpm.slotted_companyjob_id as matches_slotted_companyjob_id,
                    cjp.recency, 
                    case when cjp.recency = 1 then 'y' when cjp.recency is null then null end as most_recent,
                    cp.paymarket as composite_paymarket,
                    cjp.currency as composite_currency,
                    cp.country_code as composite_country_code,
                    sd.currency_code as survey_currency_code,
                    sd.country_code as survey_country_code,
                    cjp.effective_date as composite_effective_date,
                    case when cjpm.mdjob_code is not null then cjp.effective_date else isnull(s.effective_date,cjp.effective_date) end as survey_effective_date, 
                    cjp.rate as composite_rate,
                    x.currencyconversionfactor as composite_ccf_usd,
                    y.currencyconversionfactor as surveys_ccf_usd,
                    sd.orgs as orgs,
                    sd.incs as incs,
                    cp.country_code,
                    cp.default_scope,
                    cp.default_scope as isdefaultpaymarket,
                    cp.geo_label,
                    cp.geo_value,
                    cp.industry_label,
                    cp.industry_value,
                    cp.size_label,
                    cp.size_value,
                    cjp.aging_factor as composite_aging_factor,
                    cjpm.aging_factor as matches_aging_factor,
                    s.pfaging_factor,
                    s.survey_code, 
                    s.survey_code_short,
                    s.survey_name,
                    s.display_name,
                    --sj.job_code as surveys_job_code,
                    isnull(sj.job_code, md.job_code) as survey_job_code, 
                    cjpm.mdjob_code as matches_mdjob_code,
                    sj.job_description as surveys_job_description,
                    --sj.job_title as surveys_job_title,
                    isnull(sj.job_title, md.job_title) as survey_job_title, 
                    isnull(sj.level_code, md.job_level ) as survey_job_level,
                    sj.job_family as surveys_job_family,
                    case when cjpm.mdjob_code is not null then 'payfactors' else s.survey_publisher end as survey_publisher,
                    case when cjpm.mdjob_code is not null then cp.industry_label+':'+cp.industry_value else sd.scope1 end as scope1,
                    case when cjpm.mdjob_code is not null then cp.size_label+':'+cp.size_value else sd.scope2 end as scope2,
                    case when cjpm.mdjob_code is not null then cp.geo_label+':'+cp.geo_value else sd.scope3 end as scope3,
                    sd.scope1 as survey_scope1,
                    sd.scope2 as survey_scope2,
                    sd.scope3 as survey_scope3,
                    sd.weightingtype,
                    cjpm.weightingtype as matches_weightingtype,
                    cjpm.match_weight as matches_match_weight,
                    cjpm.match_adjustment as matches_match_adjustment,
                    cjp.composite_adjustment as composite_adjustment,
                    sd.flsaexemptpct as survey_flsaexemptpct,
                    --base
                    cjp.base_reference_point as composite_base_reference_point,
                    cjp.base10 * x.currencyconversionfactor as composite_base10,cjpm.base10 * x.currencyconversionfactor as matches_base10,sd.base10 * y.currencyconversionfactor as survey_base10,
                    cjp.base25 * x.currencyconversionfactor as composite_base25,cjpm.base25 * x.currencyconversionfactor as matches_base25,sd.base25 * y.currencyconversionfactor as survey_base25,
                    cjp.base50 * x.currencyconversionfactor as composite_base50,cjpm.base50 * x.currencyconversionfactor as matches_base50,sd.base50 * y.currencyconversionfactor as survey_base50,
                    cjpm.base75 * x.currencyconversionfactor as composite_base75,cjpm.base75 * x.currencyconversionfactor as matches_base75,sd.base75 * y.currencyconversionfactor as survey_base75,
                    cjp.base90 * x.currencyconversionfactor as composite_base90,cjpm.base90 * x.currencyconversionfactor as matches_base90,sd.base90 * y.currencyconversionfactor as survey_base90,
                    cjp.baseavg * x.currencyconversionfactor as composite_baseavg,cjpm.baseavg * x.currencyconversionfactor as matches_baseavg,sd.baseavg * y.currencyconversionfactor as survey_baseavg,
                    cjp.basemrp * x.currencyconversionfactor as composite_basemrp,cjpm.basemrp * x.currencyconversionfactor as matches_basemrp,
                    --bonus
                    cjp.bonus_reference_point as composite_bonus_reference_point,
                    cjp.bonus10 * x.currencyconversionfactor as composite_bonus10,cjpm.bonus10 * x.currencyconversionfactor as matches_bonus10,sd.bonus10 * y.currencyconversionfactor as survey_bonus10,
                    cjp.bonus25 * x.currencyconversionfactor as composite_bonus25,cjpm.bonus25 * x.currencyconversionfactor as matches_bonus25,sd.bonus25 * y.currencyconversionfactor as survey_bonus25,
                    cjp.bonus50 * x.currencyconversionfactor as composite_bonus50,cjpm.bonus50 * x.currencyconversionfactor as matches_bonus50,sd.bonus50 * y.currencyconversionfactor as survey_bonus50,
                    cjp.bonus75 * x.currencyconversionfactor as composite_bonus75,cjpm.bonus75 * x.currencyconversionfactor as matches_bonus75,sd.bonus75 * y.currencyconversionfactor as survey_bonus75,
                    cjp.bonus90 * x.currencyconversionfactor as composite_bonus90,cjpm.bonus90 * x.currencyconversionfactor as matches_bonus90,sd.bonus90 * y.currencyconversionfactor as survey_bonus90,
                    cjp.bonusavg *  x.currencyconversionfactor as composite_bonusavg,cjpm.bonusavg *  x.currencyconversionfactor as matches_bonusavg,sd.bonusavg * y.currencyconversionfactor as survey_bonusavg,
                    cjp.bonusmrp *  x.currencyconversionfactor as composite_bonusmrp,cjpm.bonusmrp *  x.currencyconversionfactor as matches_bonusmrp,
                    --bonuspct
                    cjp.bonuspct_reference_point as composite_bonuspct_reference_point,
                    cjp.bonuspct10 as composite_bonuspct10,cjpm.bonuspct10 as matches_bonuspct10,sd.bonuspct10 as survey_bonuspct10,
                    cjp.bonuspct25 as composite_bonuspct25,cjpm.bonuspct25 as matches_bonuspct25,sd.bonuspct25 as survey_bonuspct25,
                    cjp.bonuspct50 as composite_bonuspct50,cjpm.bonuspct50 as matches_bonuspct50,sd.bonuspct50 as survey_bonuspct50,
                    cjp.bonuspct75 as composite_bonuspct75,cjpm.bonuspct75 as matches_bonuspct75,sd.bonuspct75 as survey_bonuspct75,
                    cjp.bonuspct90 as composite_bonuspct90,cjpm.bonuspct90 as matches_bonuspct90,sd.bonuspct90 as survey_bonuspct90,
                    cjp.bonuspctavg as composite_bonuspctavg,cjpm.bonuspctavg as matches_bonuspctavg,sd.bonuspctavg as survey_bonuspctavg,
                    cjp.bonuspctmrp as composite_bonuspctmrp,cjpm.bonuspctmrp as matches_bonuspctmrp,
                    --bonustarget
                    cjp.bonustarget_reference_point as composite_bonustarget_reference_point,
                    cjp.bonustarget10 * x.currencyconversionfactor as composite_bonustarget10,cjpm.bonustarget10 * x.currencyconversionfactor as matches_bonustarget10,sd.bonustarget10 * y.currencyconversionfactor as survey_bonustarget10,
                    cjp.bonustarget25 * x.currencyconversionfactor as composite_bonustarget25,cjpm.bonustarget25 * x.currencyconversionfactor as matches_bonustarget25,sd.bonustarget25 * y.currencyconversionfactor as survey_bonustarget25,
                    cjp.bonustarget50 * x.currencyconversionfactor as composite_bonustarget50,cjpm.bonustarget50 * x.currencyconversionfactor as matches_bonustarget50,sd.bonustarget50 * y.currencyconversionfactor as survey_bonustarget50,
                    cjp.bonustarget75 * x.currencyconversionfactor as composite_bonustarget75,cjpm.bonustarget75 * x.currencyconversionfactor as matches_bonustarget75,sd.bonustarget75 * y.currencyconversionfactor as survey_bonustarget75,
                    cjp.bonustarget90 * x.currencyconversionfactor as composite_bonustarget90,cjpm.bonustarget90 * x.currencyconversionfactor as matches_bonustarget90,sd.bonustarget90 * y.currencyconversionfactor as survey_bonustarget90,
                    cjp.bonustargetavg * x.currencyconversionfactor as composite_bonustargetavg,cjpm.bonustargetavg * x.currencyconversionfactor as matches_bonustargetavg,sd.bonustargetavg * x.currencyconversionfactor as survey_bonustargetavg,
                    cjp.bonustargetmrp * x.currencyconversionfactor as composite_bonustargetmrp,cjpm.bonustargetmrp * x.currencyconversionfactor as matches_bonustargetmrp,
                    --bonustargetpct				
                    cjp.bonustargetpct_reference_point as composite_bonustargetpct_reference_point,
                    cjp.bonustargetpct10 as composite_bonustargetpct10,cjpm.bonustargetpct10 as matches_bonustargetpct10,sd.bonustargetpct10 as survey_bonustargetpct10,
                    cjp.bonustargetpct25 as composite_bonustargetpct25,cjpm.bonustargetpct25 as matches_bonustargetpct25,sd.bonustargetpct25 as survey_bonustargetpct25,
                    cjp.bonustargetpct50 as composite_bonustargetpct50,cjpm.bonustargetpct50 as matches_bonustargetpct50,sd.bonustargetpct50 as survey_bonustargetpct50,
                    cjp.bonustargetpct75 as composite_bonustargetpct75,cjpm.bonustargetpct75 as matches_bonustargetpct75,sd.bonustargetpct75 as survey_bonustargetpct75,
                    cjp.bonustargetpct90 as composite_bonustargetpct90,cjpm.bonustargetpct90 as matches_bonustargetpct90,sd.bonustargetpct90 as survey_bonustargetpct90,
                    cjp.bonustargetpctavg as composite_bonustargetpctavg,cjpm.bonustargetpctavg as matches_bonustargetpctavg,sd.bonustargetpctavg as survey_bonustargetpctavg,
                    cjp.bonustargetpctmrp as composite_bonustargetpctmrp,cjpm.bonustargetpctmrp as matches_bonustargetpctmrp,
                    --ltip
                    cjp.ltip_reference_point as composite_ltip_reference_point,
                    cjp.ltip10 * x.currencyconversionfactor as composite_ltip10,cjpm.ltip10 * x.currencyconversionfactor as matches_ltip10,sd.ltip10 * y.currencyconversionfactor as survey_ltip10,
                    cjp.ltip25 * x.currencyconversionfactor as composite_ltip25,cjpm.ltip25 * x.currencyconversionfactor as matches_ltip25,sd.ltip25 * y.currencyconversionfactor as survey_ltip25,
                    cjp.ltip50 * x.currencyconversionfactor as composite_ltip50,cjpm.ltip50 * x.currencyconversionfactor as matches_ltip50,sd.ltip50 * y.currencyconversionfactor as survey_ltip50,
                    cjp.ltip75 * x.currencyconversionfactor as composite_ltip75,cjpm.ltip75 * x.currencyconversionfactor as matches_ltip75,sd.ltip75 * y.currencyconversionfactor as survey_ltip75,
                    cjp.ltip90 * x.currencyconversionfactor as composite_ltip90,cjpm.ltip90 * x.currencyconversionfactor as matches_ltip90,sd.ltip90 * y.currencyconversionfactor as survey_ltip90,
                    cjp.ltipavg * x.currencyconversionfactor as composite_ltipavg,cjpm.ltipavg * x.currencyconversionfactor as matches_ltipavg,sd.ltipavg * y.currencyconversionfactor as survey_ltipavg,
                    cjp.ltipmrp * x.currencyconversionfactor as composite_ltipmrp,cjpm.ltipmrp * x.currencyconversionfactor as matches_ltipmrp,
                    --ltippct
                    cjp.ltippct_reference_point as composite_ltippct_reference_point,
                    cjp.ltippct10 as composite_ltippct10,cjpm.ltippct10 as matches_ltippct10,sd.ltippct10 as survey_ltippct10,
                    cjp.ltippct25 as composite_ltippct25,cjpm.ltippct25 as matches_ltippct25,sd.ltippct25 as survey_ltippct25,
                    cjp.ltippct50 as composite_ltippct50,cjpm.ltippct50 as matches_ltippct50,sd.ltippct50 as survey_ltippct50,
                    cjp.ltippct75 as composite_ltippct75,cjpm.ltippct75 as matches_ltippct75,sd.ltippct75 as survey_ltippct75,
                    cjp.ltippct90 as composite_ltippct90,cjpm.ltippct90 as matches_ltippct90,sd.ltippct90 as survey_ltippct90,
                    cjp.ltippctavg as composite_ltippctavg,cjpm.ltippctavg as matches_ltippctavg,sd.ltippctavg as survey_ltippctavg,
                    cjp.ltippctmrp as composite_ltippctmrp,cjpm.ltippctmrp as matches_ltippctmrp,
                    --targetltip
                    cjp.targetltip_reference_point as composite_targetltip_reference_point,
                    cjp.targetltip10 * x.currencyconversionfactor as composite_targetltip10,cjpm.targetltip10 * x.currencyconversionfactor as matches_targetltip10,sd.targetltip10 * y.currencyconversionfactor as survey_targetltip10,
                    cjp.targetltip25 * x.currencyconversionfactor as composite_targetltip25,cjpm.targetltip25 * x.currencyconversionfactor as matches_targetltip25,sd.targetltip25 * y.currencyconversionfactor as survey_targetltip25,
                    cjp.targetltip50 * x.currencyconversionfactor as composite_targetltip50,cjpm.targetltip50 * x.currencyconversionfactor as matches_targetltip50,sd.targetltip50 * y.currencyconversionfactor as survey_targetltip50,
                    cjp.targetltip75 * x.currencyconversionfactor as composite_targetltip75,cjpm.targetltip75 * x.currencyconversionfactor as matches_targetltip75,sd.targetltip75 * y.currencyconversionfactor as survey_targetltip75,
                    cjp.targetltip90 * x.currencyconversionfactor as composite_targetltip90,cjpm.targetltip90 * x.currencyconversionfactor as matches_targetltip90,sd.targetltip90 * y.currencyconversionfactor as survey_targetltip90,
                    cjp.targetltipavg * x.currencyconversionfactor as composite_targetltipavg,cjpm.targetltipavg * x.currencyconversionfactor as matches_targetltipavg,sd.targetltipavg * x.currencyconversionfactor as survey_targetltipavg,
                    cjp.targetltipmrp * x.currencyconversionfactor as composite_targetltipmrp,cjpm.targetltipmrp * x.currencyconversionfactor as matches_targetltipmrp,
                    --targettdc
                    cjp.targettdc_reference_point as composite_targettdc_reference_point,
                    cjp.targettdc10 * x.currencyconversionfactor as composite_targettdc10,cjpm.targettdc10 * x.currencyconversionfactor as matches_targettdc10,sd.targettdc10 * y.currencyconversionfactor as survey_targettdc10,
                    cjp.targettdc25 * x.currencyconversionfactor as composite_targettdc25,cjpm.targettdc25 * x.currencyconversionfactor as matches_targettdc25,sd.targettdc25 * y.currencyconversionfactor as survey_targettdc25,
                    cjp.targettdc50 * x.currencyconversionfactor as composite_targettdc50,cjpm.targettdc50 * x.currencyconversionfactor as matches_targettdc50,sd.targettdc50 * y.currencyconversionfactor as survey_targettdc50,
                    cjp.targettdc75 * x.currencyconversionfactor as composite_targettdc75,cjpm.targettdc75 * x.currencyconversionfactor as matches_targettdc75,sd.targettdc75 * y.currencyconversionfactor as survey_targettdc75,
                    cjp.targettdc25 * x.currencyconversionfactor as composite_targettdc90,cjpm.targettdc25 * x.currencyconversionfactor as matches_targettdc90,sd.targettdc90 * y.currencyconversionfactor as survey_targettdc90,
                    cjp.targettdcavg * x.currencyconversionfactor as composite_targettdcavg,cjpm.targettdcavg * x.currencyconversionfactor as matches_targettdcavg,sd.targettdcavg * x.currencyconversionfactor as survey_targettdcavg,
                    cjp.targettdcmrp * x.currencyconversionfactor as composite_targettdcrmp,cjpm.targettdcmrp * x.currencyconversionfactor as matches_targettdcrmp,
                    --tcc
                    cjp.tcc_reference_point as composite_tcc_reference_point,
                    cjp.tcc10 * x.currencyconversionfactor as composite_tcc10,cjpm.tcc10 * x.currencyconversionfactor as matches_tcc10,sd.tcc10 * y.currencyconversionfactor as survey_tcc10,
                    cjp.tcc25 * x.currencyconversionfactor as composite_tcc25,cjpm.tcc25 * x.currencyconversionfactor as matches_tcc25,sd.tcc25 * y.currencyconversionfactor as survey_tcc25,
                    cjp.tcc50 * x.currencyconversionfactor as composite_tcc50,cjpm.tcc50 * x.currencyconversionfactor as matches_tcc50,sd.tcc50 * y.currencyconversionfactor as survey_tcc50,
                    cjp.tcc75 * x.currencyconversionfactor as composite_tcc75,cjpm.tcc75 * x.currencyconversionfactor as matches_tcc75,sd.tcc75 * y.currencyconversionfactor as survey_tcc75,
                    cjp.tcc90 * x.currencyconversionfactor as composite_tcc90,cjpm.tcc90 * x.currencyconversionfactor as matches_tcc90,sd.tcc90 * y.currencyconversionfactor as survey_tcc90,
                    cjp.tccavg * y.currencyconversionfactor as composite_tccavg,cjpm.tccavg * y.currencyconversionfactor as matches_tccavg,sd.tccavg * y.currencyconversionfactor as survey_tccavg,
                    cjp.tccmrp * x.currencyconversionfactor as composite_tccmrp,cjpm.tccmrp * x.currencyconversionfactor as matches_tccmrp,
                    --tcctarget
                    cjp.tcctarget_reference_point as composite_tcctarget_reference_point,
                    cjp.tcctarget10 * x.currencyconversionfactor as composite_tcctarget10,cjpm.tcctarget10 * x.currencyconversionfactor as matches_tcctarget10,sd.tcctarget10 * y.currencyconversionfactor as survey_tcctarget10,
                    cjp.tcctarget25 * x.currencyconversionfactor as composite_tcctarget25,cjpm.tcctarget25 * x.currencyconversionfactor as matches_tcctarget25,sd.tcctarget25 * y.currencyconversionfactor as survey_tcctarget25,
                    cjp.tcctarget50 * x.currencyconversionfactor as composite_tcctarget50,cjpm.tcctarget50 * x.currencyconversionfactor as matches_tcctarget50,sd.tcctarget50 * y.currencyconversionfactor as survey_tcctarget50,
                    cjp.tcctarget75 * x.currencyconversionfactor as composite_tcctarget75,cjpm.tcctarget75 * x.currencyconversionfactor as matches_tcctarget75,sd.tcctarget75 * y.currencyconversionfactor as survey_tcctarget75,
                    cjp.tcctarget90 * x.currencyconversionfactor as composite_tcctarget90,cjpm.tcctarget90 * x.currencyconversionfactor as matches_tcctarget90,sd.tcctarget90 * y.currencyconversionfactor as survey_tcctarget90,
                    cjp.tcctargetavg * y.currencyconversionfactor as composite_tcctargetavg,cjpm.tcctargetavg * y.currencyconversionfactor as matches_tcctargetavg,sd.tcctargetavg * y.currencyconversionfactor as survey_tcctargetavg,
                    cjp.tcctargetmrp * x.currencyconversionfactor as composite_tcctargetmrp,cjpm.tcctargetmrp * x.currencyconversionfactor as matches_tcctargetmrp,
                    --tdc
                    cjp.tdc_reference_point as composite_tdc_reference_point,
                    cjp.tdc10 * x.currencyconversionfactor as composite_tdc10,cjpm.tdc10 * x.currencyconversionfactor as matches_tdc10,sd.tdc10 * y.currencyconversionfactor as survey_tdc10,
                    cjp.tdc25 * x.currencyconversionfactor as composite_tdc25,cjpm.tdc25 * x.currencyconversionfactor as matches_tdc25,sd.tdc25 * y.currencyconversionfactor as survey_tdc25,
                    cjp.tdc50 * x.currencyconversionfactor as composite_tdc50,cjpm.tdc50 * x.currencyconversionfactor as matches_tdc50,sd.tdc50 * y.currencyconversionfactor as survey_tdc50,
                    cjp.tdc75 * x.currencyconversionfactor as composite_tdc75,cjpm.tdc75 * x.currencyconversionfactor as matches_tdc75,sd.tdc75 * y.currencyconversionfactor as survey_tdc75,
                    cjp.tdc90 * x.currencyconversionfactor as composite_tdc90,cjpm.tdc90 * x.currencyconversionfactor as matches_tdc90,sd.tdc90 * y.currencyconversionfactor as survey_tdc90,
                    cjp.tdcavg * y.currencyconversionfactor as composite_tdcavg,cjpm.tdcavg * y.currencyconversionfactor as matches_tdcavg,sd.tdcavg * y.currencyconversionfactor as survey_tdcavg,
                    cjp.tdcmrp * y.currencyconversionfactor as composite_tdcmrp,cjpm.tdcmrp * y.currencyconversionfactor as matches_tdcmrp,
                    --tgp
                    cjp.tgp_reference_point as composite_tgp_reference_point,
                    cjp.tgp10 * x.currencyconversionfactor as composite_tgp10,cjpm.tgp10 * x.currencyconversionfactor as matches_tgp10,sd.tgp10 * y.currencyconversionfactor as survey_tgp10,
                    cjp.tgp25 * x.currencyconversionfactor as composite_tgp25,cjpm.tgp25 * x.currencyconversionfactor as matches_tgp25,sd.tgp25 * y.currencyconversionfactor as survey_tgp25,
                    cjp.tgp50 * x.currencyconversionfactor as composite_tgp50,cjpm.tgp50 * x.currencyconversionfactor as matches_tgp50,sd.tgp50 * y.currencyconversionfactor as survey_tgp50,
                    cjp.tgp75 * x.currencyconversionfactor as composite_tgp75,cjpm.tgp75 * x.currencyconversionfactor as matches_tgp75,sd.tgp75 * y.currencyconversionfactor as survey_tgp75,
                    cjp.tgp90 * x.currencyconversionfactor as composite_tgp90,cjpm.tgp90 * x.currencyconversionfactor as matches_tgp90,sd.tgp90 * y.currencyconversionfactor as survey_tgp90,
                    cjp.tgpavg * x.currencyconversionfactor as composite_tgpavg,cjpm.tgpavg * x.currencyconversionfactor as matches_tgpavg,sd.tgpavg * y.currencyconversionfactor as survey_tgpavg,
                    cjp.tgpmrp * x.currencyconversionfactor as composite_tgpmrp,cjpm.tgpmrp * x.currencyconversionfactor as matches_tgpmrp,
                    --fixed
                    cjp.fixed_reference_point as composite_fixed_reference_point,
                    cjp.fixed10 * x.currencyconversionfactor as composite_fixed10,cjpm.fixed10 * x.currencyconversionfactor as matches_fixed10,sd.fixed10 * x.currencyconversionfactor as survey_fixed10,
                    cjp.fixed25 * x.currencyconversionfactor as composite_fixed25,cjpm.fixed25 * x.currencyconversionfactor as matches_fixed25,sd.fixed25 * x.currencyconversionfactor as survey_fixed25,
                    cjp.fixed50 * x.currencyconversionfactor as composite_fixed50,cjpm.fixed50 * x.currencyconversionfactor as matches_fixed50,sd.fixed50 * x.currencyconversionfactor as survey_fixed50,
                    cjp.fixed75 * x.currencyconversionfactor as composite_fixed75,cjpm.fixed75 * x.currencyconversionfactor as matches_fixed75,sd.fixed75 * x.currencyconversionfactor as survey_fixed75,
                    cjp.fixed90 * x.currencyconversionfactor as composite_fixed90,cjpm.fixed90 * x.currencyconversionfactor as matches_fixed90,sd.fixed90 * x.currencyconversionfactor as survey_fixed90,
                    cjp.fixedavg * x.currencyconversionfactor as composite_fixedavg,cjpm.fixedavg * x.currencyconversionfactor as matches_fixedavg,sd.fixedavg * x.currencyconversionfactor as survey_fixedavg,
                    cjp.fixedmrp * x.currencyconversionfactor as composite_fixedmrp,cjpm.fixedmrp * x.currencyconversionfactor as matches_fixedmrp,
                    --allow
                    cjp.allow_reference_point as composite_allow_reference_point,
                    cjp.allow10 * x.currencyconversionfactor as composite_allow10,cjpm.allow10 * x.currencyconversionfactor as matches_allow10,sd.allow10 * x.currencyconversionfactor as survey_allow10,
                    cjp.allow25 * x.currencyconversionfactor as composite_allow25,cjpm.allow25 * x.currencyconversionfactor as matches_allow25,sd.allow25 * x.currencyconversionfactor as survey_allow25,
                    cjp.allow50 * x.currencyconversionfactor as composite_allow50,cjpm.allow50 * x.currencyconversionfactor as matches_allow50,sd.allow50 * x.currencyconversionfactor as survey_allow50,
                    cjp.allow75 * x.currencyconversionfactor as composite_allow75,cjpm.allow75 * x.currencyconversionfactor as matches_allow75,sd.allow75 * x.currencyconversionfactor as survey_allow75,
                    cjp.allow90 * x.currencyconversionfactor as composite_allow90,cjpm.allow90 * x.currencyconversionfactor as matches_allow90,sd.allow90 * x.currencyconversionfactor as survey_allow90,
                    cjp.allowavg * x.currencyconversionfactor as composite_allowavg,cjpm.allowavg * x.currencyconversionfactor as matches_allowavg,sd.allowavg * x.currencyconversionfactor as survey_allowavg,
                    cjp.allowmrp * x.currencyconversionfactor as composite_allowmrp,cjpm.allowmrp * x.currencyconversionfactor as matches_allowmrp,
                    --remun
                    cjp.remun_reference_point as composite_remun_reference_point,
                    cjp.remun10 * x.currencyconversionfactor as composite_remun10,cjpm.remun10 * x.currencyconversionfactor as matches_remun10,sd.remun10 * x.currencyconversionfactor as survey_remun10,
                    cjp.remun25 * x.currencyconversionfactor as composite_remun25,cjpm.remun25 * x.currencyconversionfactor as matches_remun25,sd.remun25 * x.currencyconversionfactor as survey_remun25,
                    cjp.remun50 * x.currencyconversionfactor as composite_remun50,cjpm.remun50 * x.currencyconversionfactor as matches_remun50,sd.remun50 * x.currencyconversionfactor as survey_remun50,
                    cjp.remun75 * x.currencyconversionfactor as composite_remun75,cjpm.remun75 * x.currencyconversionfactor as matches_remun75,sd.remun75 * x.currencyconversionfactor as survey_remun75,
                    cjp.remun90 * x.currencyconversionfactor as composite_remun90,cjpm.remun90 * x.currencyconversionfactor as matches_remun90,sd.remun90 * x.currencyconversionfactor as survey_remun90,
                    cjp.remunavg * x.currencyconversionfactor as composite_remunavg,cjpm.remunavg * x.currencyconversionfactor as matches_remunavg,sd.remunavg * x.currencyconversionfactor as survey_remunavg,
                    cjp.remunmrp * x.currencyconversionfactor as composite_remunmrp,cjpm.remunmrp * x.currencyconversionfactor as matches_remunmrp
                    , getdate() as loaded_on
            from cte_ppm as tp
            inner join dbo.companyjobs_pricings cjp with (nolock) 
                on tp.companyjobpricing_id = cjp.companyjobpricing_id
                and tp.company_id = cjp.company_id
            inner join dbo.companypaymarkets cp with (nolock) 
                on tp.companypaymarket_id = cp.companypaymarket_id
                and tp.company_id = cp.company_id
            inner join dbo.companyjobs_pricingsmatches cjpm with (nolock) 
                on tp.companyjobpricingmatch_id = cjpm.companyjobpricingmatch_id 
                and tp.company_id = cjpm.company_id 
            left outer join dbo.surveydata sd with (nolock) 
                on tp.survey_data_id = sd.survey_data_id
                --and tp.company_id = {input_company_id}
            left outer join dbo.surveyjob sj with (nolock) 
                on tp.survey_job_id = sj.survey_job_id
                --and tp.company_id = {input_company_id}
            left outer join dbo.surveys s with (nolock) 
                on tp.survey_id = s.survey_id
                --and tp.company_id = {input_company_id}
            left outer join mdjobs md with (nolock) 
                on cjpm.mdjob_code = md.job_code
                and cjpm.country_code = md.country_code
            left join cte_cc as x
                on x.company_id = cjp.company_id
                and x.currency_code = cjp.currency
            left join cte_cc as y
                on y.currency_code = sd.currency_code
                --and y.company_id = {input_company_id}
            where 1=1
            and tp.company_id in ({input_company_id})
            option (recompile, optimize for unknown)
	"""
    return str(sql_string).lower()


def get_pricings_model_insert_json_procedure_query() -> str:
    sql_string = """
		exec [thoughtspot].[usp_load_pricings_model_table_json] @payload=?
		"""
    return str(sql_string).lower()
