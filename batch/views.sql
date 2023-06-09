-- views.sql - create views that will return data in a format ready to display. Run in psql from the command line.

--    These views are created in the sppdata schema, which must already exists and hold data tables
--    Ideally these should drive a web API, but they can be used directly by an application for development.

\set ON_ERROR_STOP on

set search_path to sppdata;

-- drop views if exist  
drop view if exists generation_mix_piechart_vw;
drop view if exists emissions_trend_vw;
drop view if exists rtbm_lmp_map_vw;
drop view if exists da_lmp_map_vw;
drop view if exists demand_vs_forecast_vw;
drop view if exists tie_flows_long_vw; 
drop view if exists area_control_error_vw;
drop view if exists rtbm_binding_constraints_vw;

--  Feature:  current generation mix 
create view generation_mix_piechart_vw as 
with mostrecent as ( 
  select generation_mix.*, gmt_mkt_interval at time zone 'America/Chicago' as local_mkt_interval
   from sppdata.generation_mix where gmt_mkt_interval = 
    (select max(gmt_mkt_interval) from sppdata.generation_mix)
)
select local_mkt_interval, 'Hydro' as label, hydro_market+hydro_self as value from  mostrecent
union all select local_mkt_interval, 'Solar' as label, solar_market+solar_self as value from mostrecent
union all select local_mkt_interval, 'Wind' as label, wind_market+wind_self as value from mostrecent
union all select local_mkt_interval, 'Nuclear' as label, nuclear_market+nuclear_self as value from mostrecent
union all select local_mkt_interval, 'Diesel' as label, diesel_fuel_oil_market+diesel_fuel_oil_self as value from mostrecent
union all select local_mkt_interval, 'Coal' as label, coal_market+coal_self as value from mostrecent
union all select local_mkt_interval, 'Natural Gas' as label, natural_gas_market+natural_gas_self as value from mostrecent
union all select local_mkt_interval, 'Other' as label, waste_disposal_services_market+waste_disposal_services_self +
  waste_heat_market+waste_heat_self+other_market+other_self as value from mostrecent
;


--  Feature:  emissions trend 
create view emissions_trend_vw as 
with mostrecent as ( 
  select * from sppdata.generation_mix 
  -- start by selecting the most recent 7 days of data; we'll need this to calculate the weekly average
  where gmt_mkt_interval > current_timestamp - interval '7 days' 
)
, calcs as (
    select 
    gmt_mkt_interval,
    -- calculate total generation by interval
      (hydro_market+hydro_self) 
    + (solar_market+solar_self)
    + (wind_market+wind_self)
    + (nuclear_market+nuclear_self) 
    + (diesel_fuel_oil_market+diesel_fuel_oil_self)
    + (coal_market+coal_self) 
    + (natural_gas_market+natural_gas_self)
    + (waste_disposal_services_market+waste_disposal_services_self + 
       waste_heat_market+waste_heat_self+other_market+other_self) 
    as total_generation, 
    -- calculate total emissions by interval by fuel type
    (coal_market+coal_self) * 1000 * 2.26 as coal_co2_lbs,
    (natural_gas_market+natural_gas_self) * 1000 *  0.97 as natural_gas_co2_lbs,
    (diesel_fuel_oil_market+diesel_fuel_oil_self) * 1000 * 2.44 as fuel_oil_co2_lbs 
    from mostrecent
)
--  from calcs, average the weekly emissions to tell if current conditions are above or below recent conditions 
, calcsavg as (
    select 
    -- calculate the average emissions across the data (one week as limited in the mostrecent CTE)
      avg((calcs.coal_co2_lbs + calcs.natural_gas_co2_lbs + calcs.fuel_oil_co2_lbs) / calcs.total_generation) / 1000.0 
    as weekly_average
    from calcs
)
select 
calcs.gmt_mkt_interval at time zone 'America/Chicago' -- as "Local Time",
as local_mkt_interval,
round((calcs.coal_co2_lbs + calcs.natural_gas_co2_lbs + calcs.fuel_oil_co2_lbs) / calcs.total_generation) / 1000.0 
as lbs_co2_per_kwh,
calcsavg.weekly_average
from calcs
cross join calcsavg -- only one value the same for all timepoints, creating a horizontal line on the graph 
order by local_mkt_interval
;

-- Feature:  RTBM LMP map 
create view rtbm_lmp_map_vw as 
with mostrecent as ( 
  select * from sppdata.rtbm_lmp_by_location
  where gmtinterval_end = 
    (select max(gmtinterval_end) from sppdata.rtbm_lmp_by_location)
)
select 
to_char(mostrecent.gmtinterval_end at time zone 'America/Chicago', 'DD-Mon HH24:MI') as rtbm_interval_ending,
mostrecent.pnode, 
mostrecent.lmp, 
mostrecent.mcc, 
mostrecent.mlc, 
sl.settlement_location, 
sl.est_latitude,
sl.est_longitude,
sl.inferred_location_type, 
-- originally I was setting the size based on whether the location was inferred or exact, but that was not useful. 
-- case when sl.latitude is not null then 0.2 else 0.1 end as "size",
-- TODO: determine if size should be based on some other useful attribute.
0.2 as size
from sppdata.settlement_location sl
join mostrecent on (mostrecent.settlement_location = sl.settlement_location)
;

-- Feature:  DAMKT LMP map
create view da_lmp_map_vw as
with mostrecent as (
  select * from sppdata.da_lmp_by_location
  where gmtinterval_end =
  -- this is tricky. We want to display the DA hour ending that the RT interval end falls in. 
  -- To do this we need to convert from 5 minute interval ending to hour ending. 
  -- so 06:00 is 06:00, but 06:05 to 06:550 becomes 07:00. 
  date_trunc('hour', (select max(gmtinterval_end) from sppdata.rtbm_lmp_by_location) + interval '55 minutes')
)
select
to_char(mostrecent.gmtinterval_end at time zone 'America/Chicago', 'DD-Mon HH24:MI')   as da_hour_ending,
mostrecent.pnode, 
mostrecent.lmp, 
mostrecent.mcc, 
mostrecent.mlc, 
sl.settlement_location, 
sl.est_latitude,
sl.est_longitude,
sl.inferred_location_type, 
-- originally I was setting the size based on whether the location was inferred or exact, but that was not useful. 
-- case when sl.latitude is not null then 0.2 else 0.1 end as "size",
-- TODO: determine if size should be based on some other useful attribute.
0.2 as size
from sppdata.settlement_location sl
join mostrecent on (mostrecent.settlement_location = sl.settlement_location)
;

-- Feature: Demand vs. Forecast display
-- possible improvement: just return this in Wide format instead of using trick SQL to make it long format
create or replace view demand_vs_forecast_vw as 
with report_times as (
      select date_trunc('day', current_timestamp, 'America/Chicago') as report_begin, 
      date_trunc('day', current_timestamp + interval '1 day', 'America/Chicago') as report_end
) 
, mtlf as (
    select gmtinterval_end,
      mtlf
    from sppdata.mtlf_vs_actual where gmtinterval_end > (select report_begin from report_times)
                          and gmtinterval_end <= (select report_end from report_times)
)
, stlf as (
    select gmtinterval_end,
      stlf, 
      actual 
    from sppdata.stlf_vs_actual where gmtinterval_end > (select report_begin from report_times)
                          and gmtinterval_end <= (select report_end from report_times)
)

select stlf.gmtinterval_end at time zone 'America/Chicago' as interval_ending, -- as "Interval Ending", 
'Short-Term Load Forecast' as measure, 
stlf.stlf as mw 
from stlf

union all 

select stlf.gmtinterval_end at time zone 'America/Chicago' as "Interval Ending", 
'Demand' as measure, 
stlf.actual as mw 
from stlf

union all 

select mtlf.gmtinterval_end at time zone 'America/Chicago' as "Interval Ending", 
'Mid-Term Load Forecast' as measure, 
mtlf.mtlf as mw  
from mtlf

order by interval_ending
; 

-- Feature: Tie flows display
-- new change-resilient LONG format data - the data load process was changed to pivot the data into long format before loading into the database
-- this is a much more efficient way to query the data, and also allows for more flexibility in the future
create or replace view tie_flows_long_vw as
select gmttime at time zone 'America/Chicago' as local_time,
area,
mw
from sppdata.tie_flows_long
where gmttime > current_timestamp - interval '2 hours'
and gmttime < current_timestamp + interval '30 minutes'
order by area, gmttime
;

-- Feature: Area control error display
create or replace view area_control_error_vw as
select gmttime at time zone 'America/Chicago' as "local_time",
value as mw 
from sppdata.area_control_error
where gmttime > current_timestamp - interval '2 hours'
order by gmttime
;

-- Feature: RTBM binding constraints display
create or replace view rtbm_binding_constraints_vw as
select gmtinterval_end at time zone 'America/Chicago' as interval_ending,
constraint_name, 
constraint_type, 
-- nercid as "NERC ID",
-- tlr_level as "TLR Level",
-- state as "State",
shadow_price,
monitored_facility, 
contingent_facility 
from sppdata.rtbm_binding_constraints
where gmtinterval_end = (select max(gmtinterval_end) from rtbm_binding_constraints)
-- avoid returning stale data if ETL has failed 
and gmtinterval_end > current_timestamp - interval '1 hours'
order by shadow_price , constraint_type desc, 
monitored_facility, contingent_facility, constraint_name
;

-- set timing on in psql: 
\timing

-- time execution, look for slow queries
select count(*) from generation_mix_piechart_vw limit 1;
select count(*) from emissions_trend_vw_orig limit 1;
select count(*) from emissions_trend_vw limit 1;
select count(*) from rtbm_lmp_map_vw limit 1;
select count(*) from demand_vs_forecast_vw limit 1;
select count(*) from tie_flows_long_vw limit 1; 
select count(*) from area_control_error_vw limit 1;
select count(*) from rtbm_binding_constraints_vw limit 1;

