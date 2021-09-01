-- Get counts and difference in counts by database 
-- replicate grants_to_roles from source(DBA_METADATA) to target (DBA_METADATA_REPLICA)
with tgt as 
(
    select 
        'abs_edm_dev_west' as data_source 
        , table_catalog
        --, grantee_name
        , count(*) record_count 
    from snowflake.account_usage.grants_to_roles gr
    where deleted_on is null
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA', 'EDM_DCAT_QA') 
    group by 1,2 
), 
src as (
    select 
    'abs_itds_dev_east' as data_source 
    , table_catalog
    --, grantee_name
    , count(*) record_count 
    from dba_metadata_replica.dba_objects.grants_to_roles gr
    where deleted_on is null
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA', 'EDM_DCAT_QA') 
    group by 1,2 
)
select 
    src.data_source as src_data_source
    , src.table_catalog as src_table_catalog 
    , tgt.data_source 
    , tgt.table_catalog
    , src.record_count as src_record_count 
    , tgt.record_count 
    , coalesce(src.record_count,0) - coalesce(tgt.record_count,0) as record_count_diff 
from src 
full outer join tgt 
    on src.table_catalog = tgt.table_catalog 
;

-- Get counts and difference in counts by object type 
with diff_records as (
select distinct
    table_catalog
    , table_schema
    , name 
    , granted_on
    , granted_to 
    , grantee_name 
    , grant_option 
    , privilege 
from dba_metadata_replica.dba_objects.grants_to_roles gr
    where deleted_on is null
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA')
    --and table_catalog  = 'EDM_ANALYTICS_QA'
    and deleted_on is null 
minus 
select distinct
    table_catalog
    , table_schema
    , name 
    , granted_on
    , granted_to 
    , grantee_name 
    , grant_option 
    , privilege 
from snowflake.account_usage.grants_to_roles gr
    where deleted_on is null 
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA')
    --and table_catalog  = 'EDM_ANALYTICS_QA'
    and deleted_on is null 
)
select 
 --gr1.table_catalog, gr1.table_schema, gr1.granted_on, gr1.created_on, gr1.name
 --, count(distinct gr1.name)
 gr1.granted_on, count(*), min(created_on) as min_created_on, max(created_on) as max_created_on 
from diff_records dr 
inner join dba_metadata_replica.dba_objects.grants_to_roles gr1
    on dr.table_catalog = gr1.table_catalog 
    and dr.table_schema = gr1.table_schema 
    and dr.name = gr1.name 
    and dr.granted_on = gr1.granted_on 
    and dr.granted_to = gr1.granted_to 
    and dr.grantee_name = gr1.grantee_name 
    and dr.grant_option = gr1.grant_option 
    and dr.privilege = gr1.privilege 
    --and dr.name = 'COUNTCHECKS_SRC_TO_TGT'
--where gr1.granted_on = 'TABLE'
group by 1;

-- detailed query for debugging 
-- get details of mismatched privileges for object_type = 'TABLE'
with diff_records as (
select distinct
    table_catalog
    , table_schema
    , name 
    , granted_on
    , granted_to 
    , grantee_name 
    , grant_option 
    , privilege 
from dba_metadata_replica.dba_objects.grants_to_roles gr
    where deleted_on is null
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA')
    --and table_catalog  = 'EDM_ANALYTICS_QA'
    and deleted_on is null 
minus 
select distinct
    table_catalog
    , table_schema
    , name 
    , granted_on
    , granted_to 
    , grantee_name 
    , grant_option 
    , privilege 
from snowflake.account_usage.grants_to_roles gr
    where deleted_on is null 
    and table_catalog in ('EDM_CONFIRMED_QA', 'EDM_CONFIRMED_OUT_QA', 'EDM_ANALYTICS_QA', 'EDM_REFINED_QA', 'EDM_FEATURES_QA',
        'EDM_VIEWS_QA', 'EDM_MONITORING_QA')
    --and table_catalog  = 'EDM_ANALYTICS_QA'
    and deleted_on is null 
)
select 
 distinct gr1.table_catalog, gr1.table_schema, gr1.granted_on, gr1.created_on, gr1.name, gr1.privilege
 , t.table_name 
from diff_records dr 
inner join dba_metadata_replica.dba_objects.grants_to_roles gr1
    on dr.table_catalog = gr1.table_catalog 
    and dr.table_schema = gr1.table_schema 
    and dr.name = gr1.name 
    and dr.granted_on = gr1.granted_on 
    and dr.granted_to = gr1.granted_to 
    and dr.grantee_name = gr1.grantee_name 
    and dr.grant_option = gr1.grant_option 
    and dr.privilege = gr1.privilege 
    --and dr.name = 'COUNTCHECKS_SRC_TO_TGT'
left outer join snowflake.account_usage.tables t 
    on t.table_catalog = dr.table_catalog 
    and t.table_schema = dr.table_schema 
    and t.table_name = dr.name 
where gr1.granted_on = 'TABLE'
and gr1.created_on < '2020-12-12'
--and (case when gr1.granted_on = 'TABLE' then (t.table_name is not null) else 1 end)
order by created_on asc;
