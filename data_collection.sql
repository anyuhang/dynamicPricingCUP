-- SEM
   select cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer) as mktkey, ad_mkt_id as ad_market_id,ppc_amount_usd as cup,
          MAX(adm.ad_mkt_state_name) as ad_state, MAX(adm.ad_mkt_regn_name) as ad_region, MAX(adm.ad_mkt_speclty_name) as ad_specialty,
          SUM(cast(quality_score as double) * cast(external_clicks as integer))/SUM(cast(external_clicks as integer)) as avg_quality_score,
          SUM(cast(avg_position as double)*cast(external_impressions as integer))/SUM(cast(external_impressions as integer)) as avg_position,
          count(distinct event_date) as numDays, sum(cast(external_clicks as integer)) as exClicks,
          sum(cast(external_impressions as integer)) as imp, sum(cast(external_cost as double)) as cost,
          sum(cast(external_clicks as integer))/sum(cast(external_impressions as integer)) as exCTR,
          sum(cast(external_cost as double))/sum(cast(external_clicks as integer)) as CPC,
          SUM(cast(internal_clicks_value as double)) as ad_click_value,SUM(cast(internal_ad_contact_value as double)) as ad_contact_value,
          SUM(cast(internal_ad_contact_value as double))/ppc_amount_usd as acu,
          (SUM(cast(internal_ad_contact_value as double))/ppc_amount_usd)/sum(cast(external_clicks as integer)) as acu_cvt
   from dm.adwords_keyword_performance_pbx a join dm.ad_mkt_dim adm on
        adm.ad_mkt_key = cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer)
        join dm.ad_pricing_ppc c on adm.ad_mkt_id=c.ad_market_id
   where event_date >= '2017-01-01' and account like '%|AdBlock%'
   group by 1,2,3

-- census data
 select ad_region_id, top_state,county,sum(number_sex_and_age_total_population) as population,
        sum(number_sex_and_age_total_population*(total_population_20_to_24_years+total_population_25_to_29_years+ total_population_30_to_34_years))/sum(number_sex_and_age_total_population) as young,
        sum(number_sex_and_age_male_population)/sum(number_sex_and_age_total_population) as male,
        sum(number_hispanic_or_latino_total_population_hispanic_or_latino_of_any_race)/sum(number_hispanic_or_latino_total_population) as hispanicPct,
        sum(number_households_by_type_total_households) as households,
        sum(total_population_for_education) as eduTotal, sum(estimate_total_less_than_high_school_graduate+ estimate_total_high_school_graduate_includes_equivalency) as nocollege,
        sum(estimate_total_less_than_high_school_graduate+ estimate_total_high_school_graduate_includes_equivalency)/sum(total_population_for_education) as nocollegePct,
        sum(estimate_income_and_benefits_in_2014_inflation_adjusted_dollars_total_households_median_household_income_dollars*number_households_by_type_total_households)/sum(number_households_by_type_total_households) as hhIncome,
        sum(estimate_health_insurance_cov_civ_noninst_population_nohealth_insurance_cov)/sum(estimate_health_insurance_cov_civ_noninst_population_total) as noinsurancePct,
        sum(estimate_separated+ estimate_divorced)/sum(marital_status_total_population) as divoiced,
        sum(estimate_income_in_the_past_12_months_below_poverty_level)/sum(poverty_level_total_population) as poverty
   from dm.census_data group by 1,2,3 order by 1

-- sellthrough
   drop table tmp_data_dm.yan_amm_sellthrough;
 create table tmp_data_dm.yan_amm_sellthrough as
 select ad_market_id, a.year_month, sl_inventory, sl_price, sl_sold_inventory, sl_sold_value,sl_unsold_inventory,
        sl_unsold_value, sl_sell_through,lawyer_cnt, lawyer_claimed_cnt, market_size, ad_monetization_status
   from dm.hist_market_intelligence_detail a
   join (select year_month, max(etl_load_date) as endmonth
           from dm.hist_market_intelligence_detail
          where year_month>201610
          group by 1) b
     on a.year_month=b.year_month and a.etl_load_date=b.endmonth
  where market_type='Block'

with churn as (select market_id as ad_market_id, count(yearmonth) as months, sum(churned)/count(yearmonth) as churnMonthRate,
                        sum(-mrr_churned) as mrr_churned,sum(mrr_total) as mrr_total,sum(-mrr_churned)/sum(mrr_total) as churnRate
                   from (select yearmonth,market_id, sum(mrr_churned) as mrr_churned, sum(mrr_total) as mrr_total,
                            case when sum(mrr_churned)/sum(mrr_total)<=-0.03 then 1 else 0 end as churned
                       from dm.mrr_market_classification a join tmp_data_dm.yan_mkt_inventory b on a.market_id=b.ad_target_id
                      where yearmonth>=201610
                      group by yearmonth,market_id ) aaa
                  where mrr_total>0
                  group by 1),
 lawyers as (select market_id as ad_market_id, count(distinct customer_id) as lawyers
               from dm.mrr_market_classification a join tmp_data_dm.yan_mkt_inventory b on a.market_id=b.ad_target_id
              where yearmonth=201703 and customer_category!='CHURNED' group by market_id),
 sellthrough as (select ad_market_id,sl_inventory,sl_sold_inventory,sl_sell_through
                  from tmp_data_dm.yan_amm_sellthrough
                 where year_month=201703),
 sem as (select cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer) as mktkey, ad_mkt_id as ad_market_id,
          ad_region_id,ppc_amount_usd as cup,
          MAX(adm.ad_mkt_state_name) as ad_state, MAX(adm.ad_mkt_regn_name) as ad_region, MAX(adm.ad_mkt_speclty_name) as ad_specialty,
          SUM(cast(quality_score as double) * cast(external_clicks as integer))/SUM(cast(external_clicks as integer)) as avg_quality_score,
          SUM(cast(avg_position as double)*cast(external_impressions as integer))/SUM(cast(external_impressions as integer)) as avg_position,
          count(distinct event_date) as numDays, sum(cast(external_clicks as integer)) as exClicks,
          sum(cast(external_impressions as integer)) as imp, sum(cast(external_cost as double)) as cost,
          sum(cast(external_clicks as integer))/sum(cast(external_impressions as integer)) as exCTR,
          sum(cast(external_cost as double))/sum(cast(external_clicks as integer)) as CPC,
          SUM(cast(internal_clicks_value as double)) as ad_click_value,SUM(cast(internal_ad_contact_value as double)) as ad_contact_value,
          SUM(cast(internal_ad_contact_value as double))/ppc_amount_usd as acu,
          (SUM(cast(internal_ad_contact_value as double))/ppc_amount_usd)/sum(cast(external_clicks as integer)) as acu_cvt
   from dm.adwords_keyword_performance_pbx a join dm.ad_mkt_dim adm on
        adm.ad_mkt_key = cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer)
        join ad_market_dimension b on adm.ad_mkt_id=b.ad_market_id
        join dm.ad_pricing_ppc c on adm.ad_mkt_id=c.ad_market_id
   where event_date >= '2017-01-01' and account like '%|AdBlock%'
   group by 1,2,3,4)
 select months, churnMonthRate, mrr_churned, mrr_total, churnRate, lawyers, sl_inventory,sl_sold_inventory
          ,sl_sell_through, a.*
   from sem a
   join churn d on d.ad_market_id=a.ad_market_id
   join lawyers e on e.ad_market_id=a.ad_market_id
   join sellthrough f on f.ad_market_id=a.ad_market_id

-- specialty & keywords
with kw as (select specialty_name, keyword,sum(cast(external_impressions as int)) as imp,
    sum(case when external_cost is not null then cast(external_cost as double) else 0 end)/sum(case when external_clicks is not null then cast(external_clicks as int) else 1 end) as ppc
 from dm.adwords_keyword_performance_pbx a join dm.ad_mkt_dim adm on
       adm.ad_mkt_key = cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer)
       join dm.ad_market_dimension b on adm.ad_mkt_id=b.ad_market_id
       join dm.specialty_dimension c on b.specialty_id=c.specialty_id
where event_date >= '2017-01-01' and account like '%|AdBlock%'
group by 1,2)
select * from
  (select specialty_name, keyword,imp, ppc, row_number() over(partition by specialty_name order by imp desc) as r from kw) aa
  where r<=5 order by 1

select distinct ad_mkt_id as ad_market_id,adm.ad_mkt_regn_name as ad_region, adm.ad_mkt_speclty_name as specialty,
       regexp_replace(trim(split_part(split_part(adm.ad_mkt_regn_name, '-',1), 'County', 1 )),' ','+') as geo1,
       regexp_replace(trim(split_part(adm.ad_mkt_regn_name, '-',1)),' ','+') as geo2
from dm.adwords_keyword_performance_pbx a join dm.ad_mkt_dim adm on
     adm.ad_mkt_key = cast(regexp_extract(campaign,'[0-9]+$|[0-9]+[0-9]',0) as integer)
     join dm.ad_pricing_ppc c on adm.ad_mkt_id=c.ad_market_id
where event_date >= '2017-01-01' and account like '%|AdBlock%'

