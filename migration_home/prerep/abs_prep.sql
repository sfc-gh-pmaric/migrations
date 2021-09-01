use role securityadmin;

-- this preparation job is an example script for target account where you run generated scripts
-- the main point is that role needs to have MANAGE GRANTS privilege at minimum, accountadmin will work of course.

create role if not exists  repadmin ;
grant MANAGE GRANTS on account to role repadmin;
use role accountadmin;
-- grant role  accountadmin to role repadmin ;
grant IMPORTED PRIVILEGES on database snowflake to role repadmin ;
grant create database on account to role repadmin ;
grant create warehouse on account to role repadmin ;

grant role repadmin to user renga_sfc;
grant role repadmin to user matwa00;
grant role repadmin to user rbala22;

grant usage on database dba_metadata to role repadmin;

use role sysadmin;
use database dba_metadata;

grant create schema on database dba_metadata to role repadmin;

use role repadmin;
use database dba_metadata;

create schema sc_crossrep;
