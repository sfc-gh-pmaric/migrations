Current Syntax: ALTER DATABASE <name> GLOBAL;
New Syntax: ALTER DATABASE <name> ENABLE REPLICATION TO ACCOUNTS <account>[,<account>..];
ALTER DATABASE <name> ENABLE FAILOVER TO ACCOUNTS account>[,<account>..];
 Also, now you can create a secondary database by using the primary database name instead of replication group id.
Current Syntax: CREATE GLOBAL DATABASE <name> REPLICATION GROUP <replication_group_id>
New Syntax: CREATE DATABASE <name> AS REPLICA OF <primary_db_name>;
Show global databases command has changed along with its output columns.
Current Syntax: SHOW GLOBAL DATABASES;
New Syntax: SHOW REPLICATION DATABASES;

==> Changed script for the new Syntax [Oct 23th, 2019]

Pre-replication Preparation:
- python 3
- snowSQL and Python connector
- run prep.sql job (modify xxx to your user name in stand-by account, it's just an example jobs that may needed for role, task/SP or manual replication. If you are using snowflake replicaiton engine and role accountadmin, not use task/SPs, you will need to run this job at all. )
- create database list in a text file under MIGRATION_HOME folder, the file contains the list of database, one database per line  
- run genPrep.py to generate tasks and resume/suspend tasks 
- pick up environment variable by running 'source env_xxx.sh' 
where crossrep is the package created 

Command options description in python main.py -x where -x in one of the following:
  -u:           to create a list of user tables to store account_usage data in schema AU_DATABASE and database AU_SCHEMA of source snowflake account. This database can be replicated to target account.
  -c all:       to get incremental updates of metadata 
  -b:           to Generate account privige grant commands 
  -ddl all:     to create ddl for all databases in the account (manual replication only) 
      filename: file contains the list of databases whose DDLs need to be created, one line per database in the file. 
  -dis:         to discover how many objects in the account
  -elt all:     to create elt jobs for all databases in the account (manual replication only)   
      filename: file contains the list of databases whose DDLs need to be created, one line per database in the file. 
  -e num:       to generate report, alter/drop/ddl statement for reference objects that needs to be handled ahead of replication (cross-database referrenced and ID-based objects)
                num is a version number to generate a different version file in case you need to execute multiple times so you don't override the previous generated files 
  to Generate account-level objects and their grants of ownership and privileges (-o)
  -f:           to to generate future grants
  -fr:          to disable and enable all users, suspend or resume warehouses when you want to use those commands to freeze your source account during cut-over time of switching source and target account  
  -g all:       to generate ALTER DATABASE ENABLE REPLICATION statements for all databases 
    filename:   file contains database list of which needs to be replicated, one line per database in the file 
  -l all:       to generate 4 statements that 
                1. create target database linking to source database, 
                2. 'alter database refresh' refreshing statement 
                3. monitor replication progress statement 
                4. 'alter database xxx primary' switching secondary db to primary  
    filename:   file contains database list of which needs to be replicated, one line per database in the file 
  -mon:         executing monitoring queries on target account in PROD or customer account
  -o olist:     to generate grants on account level objects, olist is one of multiple of ['WAREHOUSE', 'NETWORK_POLICY', 'RESOURCE_MONITOR'], space as delimiter  
  -p :          to Generate account level parameters 
  -pipe all:    to generate pipe DDL and their grants for all databases in account
    filename:   file contains database list of whose pipe DDLs need to be generated, one line per database in the file
  -r nopwd/samepwd/randpwd: to Generate users/roles and their relationships with options of no password, same static password(cr0ss2REP), or random password 
  -stage all:    to generate external stage DDL and their grants for all databases in account
    filename:   file contains database list of whose stages DDLs need to be generated, one line per database in the file
  -t:           testing by generating dropping statement of all created objects in target account so it can be rerun 
  -val filename:to validate row count and agghash for each tables between target and source account, need to connect to both

Step 1. Modify env_xxx.sh for environment variable used by python script:
SRC_PROD_xxx is environment variable for snwoflake PROD account where customer source account locates (snowflake internal only)
SRC_CUST_xxx is environment variable for customer source account 
TGT_CUST_xxx is environment variable for customer target account 
SNOWHOUSE_xxx is environment variable for SNOWHOUSE (snowflake internal only)

  SRC_PROD_USER=MYANG                                       => default connecting user name to connect to snwoflake PROD account  
  SRC_PROD_ACCOUNT=snowflake.va.us-east-1.external-zone     => snwoflake PROD account 
  SRC_PROD_ROLE=DATA_OPS_RL                                 => default connecting role for snowflake PROD account 
  SRC_PROD_DATABASE=SCRATCH                                 => default connecting database for snowflake PROD account, needs to exist (will create tables in this DB to store metadata)
  SRC_PROD_SCHEMA=MIGRATION                                 => default connecting schema for snowflake PROD account, create if not existing (will create tables in this DB to store metadata)
  SRC_PROD_WAREHOUSE=SUPPORT_2XL                            => default connecting warehouse for snowflake PROD account, needs to exist 
  SOURCE_ACCOUNT=MIGRATION                                  => source account name that metadata is collected against, only needed in SNOWFLAKE mode, leave it empty in CUSTOMER mode   
 
  SRC_CUST_USER=MYANG                                       => user name to connect to source account                                      
  SRC_CUST_ACCOUNT=migration.us-east-1                      => default connecting source account 
  SRC_CUST_PWD=xxx                                          => default connecting user password for source account 
  SRC_CUST_ROLE=ACCOUNTADMIN                                => default connecting user role for source account 
  SRC_CUST_DATABASE=DB_CROSSREP                             => default connecting database name, to be used to constain metadata of customer primary account, create if not existing
  SRC_CUST_SCHEMA=SC_CROSSREP                               => default connecting schema name, to be used to contain metadata of customer primary account, create if not existing 
  SRC_CUST_WAREHOUSE=WH_CROSSREP                            => default connecting warehouse to use, needs to exist
  
  TGT_CUST_USER=MYANG                                       => user name to connect to target account                                       
  TGT_CUST_ACCOUNT=migration									              => target account 
  TGT_CUST_PWD=xxx                                          => user password for target account 
  TGT_CUST_ROLE=REPADMIN                                    => user role for target account  
  TGT_CUST_DATABASE=DB_CROSSREP                             => database name to be used to constain metadata of customer primary account in target, needs to exist
  TGT_CUST_SCHEMA=SC_CROSSREP                               => schema name to be used to contain metadata of customer primary account in target, create if not existing 
  TGT_CUST_WAREHOUSE=WH_CROSSREP                            => warehouse to use, needs to exist
  
  TARGET_ROLE=REPADMIN                                      => custom role used for replication on customer standby account 
  MIGRATION_HOME=/Users/myang/crossreplication/prod/        => home folder for this python package code 
  REP_MODE=CUSTOMER                                         => tell whether this is executing by CUSTOMER or SNOWFLAKE or SNOWHOUSE, default mode (can be overriden using option -m )
  AU_DATABASE=DB_CROSSREP                                   => database name for storing account_usage information in source account, create if not existing, only at CUSTOMER mode  
  AU_SCHEMA=SC_ACCTUSAGE                                    => schema name for storing account_usage information in source account, create if not existing, only at CUSTOMER mode  
  REP_ACCT_LIST=aws_us_east_1.myaccount2                    => account list to be enabled on replication
  FAILOVER_ACCT_LIST=aws_us_east_1.myaccount2               => account list to be enabled on failover
[can have multile region.account, separate by ",", no space in between]
[REP_ACCT_LIST=aws_us_east_1.myaccount2,azure_westeurope.myaccount3]

  SNOWHOUSE_USER=MYANG
  SNOWHOUSE_ACCOUNT=SNOWHOUSE 
  SNOWHOUSE_ROLE=SNOWHOUSE_PHI_RO_RL
  SNOWHOUSE_DATABASE=SNOWHOUSE_IMPORT
  SNOWHOUSE_SCHEMA=VA 
  SNOWHOUSE_WAREHOUSE=SNOWHOUSE 
  SNOWHOUSE_DEPLOYMENT=VA
  SNOWHOUSE_CUST_ACCOUNT=ZETAGLOBAL 
 in a terminal command line, run 'source env_xxx.sh' to pick up environment variables
 
//FVFXL0E8HV2G:migration_home rsreenivasan$ python3 main.py -u
//FVFXL0E8HV2G:migration_home rsreenivasan$ python3 main.py -c 1

Step 2: to create all metadata needed initially (first run, 2nd or later run will have incremental updates)
  1) open 3 terminals, in each terminal, source env_xxx.sh
  2) run one of the following in each terminal
  python main.py -c 1
  python main.py -c 2
  python main.py -c 3
  3) once first one (python main.py -c 1) completes, run the following in parallel in 2 terminals (run 'source env_xxx.sh' if new terminal)
  python main.py -c 4
  python main.py -c 5
  4) once option 3 completes (python main.py -c 3 ), run the following (in parallel):
  python main.py -c 6

  or you can run all of metadata creation :
  python main.py -c all

  This step will generate the following user created table in the database of SRC_CUST_DATABASE and schema of SRC_CUST_SCHEMA:
  ALL_ROLES               => store all roles information
  ALL_USERS
  ALL_WAREHOUSES
  ALL_RESOURCEMONITORS
  ALL_NETWORKPOLICIES     => all network policies, only available on CUSTOMER mode (jira)
  ALL_SHARES
  ALL_SEQ_DEFAULT
  PARENT_CHILD
  PRIVILEGES
  ALL_DATABASES
  ALL_SCHEMAS
  TABLE_CONSTRAINTS
  ALL_PARMS
  
  === > created later if needed for option -f, -stage, -pipe , -l respectively
  ALL_FGRANTS
  ALL_STAGES
  ALL_PIPES
  ALL_GLOBAL_DBS

Step 3: Generating cross-database foreign-keys, default sequence handling files and Materialized views (including invalid MVs),  and all external tables 
python main.py -e 1

The following files are generated:
  a1_fk_DDL_xxx.sql 
  a1_drop_fk_xxx.sql
  a1_add_fk_xxx.sql
  a2_drop_default_xxx.sql
  a2_alter_default_xxx.sql
  a3_drop_extb_xxx.sql
  a3_extb_DDL_xxx.sql
  a3_drop_MV_xxx.sql
  a3_crossdb_MVDDL_xxx.sql

Step 4: while customer is close to start replication, ask them to run the following sql files :
  a1_drop_fk_xxx.sql
  a2_alter_default_xxx.sql (if ok to alter, if not, need to drop them by running a2_drop_default_xxx.sql)
  a3_drop_extb_xxx.sql
  a3_drop_MV_xxx.sql

In the same time, ask custoemr to run the following (CUST mode)
[From  customer account: 
- ACCOUNT_USAGE to be created
- network policies to be created (SNOW-83395)
]

Replication :

Step 5: Generating ALTER database enable replication/failover statements for all databases or database names from a file (by default in the MIGRATION_HOME folder)
python main.py -g all
python main.py -g <dbfile>  ==> dbfile is file name that contains database list in MIGRATION_HOME folder (one database each line in the file)

The following files are generated:
  b1_alter_replica_dbs.sql
  b1_alter_failover_dbs.sql

Step 6: Ask Customer to execute 'ALTER database xxx' generated from previous steps on customer's primary account
  snowsql -f b1_alter_replica_dbs.sql

Step 7: Generating sql commands to create and link standby databases into replication group from previous run, 
as well as generate statement to refresh standby database, monitor progress of refreshing and switch over standby databases to primary 
python main.py -l dbfile 

The following file is generated:
b2_create_standby_global.sql
b3_refresh_all_global.sql
b4_monitor_last_refresh.sql
b5_switchover_to_standby.sql
prod_monitor_last_refresh.sql

Step 8: Ask Customer to execute sql commands generated from previous step on customer's stand-by accounts to create and link standby databases 
snowsql -f b2_create_standby_global.sql


Step 9: Ask Customer to execute sql commands of refresh standby databases on customer's stand-by accounts 
snowsql -f b3_refresh_all_global.sql

Step 10: Ask Customer to execute sql commands of monitor progress of refreshing on customer's stand-by accounts periodically till refreshing is complete
snowsql -f b4_monitor_last_refresh.sql

step 11: for manual replication, using unloading and loading jobs intead of snowflake replication engine
---- adding steps for manual migration: 
1) define file format and stage name, provide the names in the __init__.py file, such as:
fileformat_name='mycsvformat'
stage_name='sfcsupport_csv'
where both are assumed to be pre-created in the same connection DB and schema.

2) run ddl and elt option
python main.py -ddl dbfile
python main.py -elt dbfile 

They will generate DDLs in /ddl/ folder and ELT jobs in /elt/ folder

3) run generated DDLs in the target (may need multiple runs due to dependencies such as view with underlying table not created yet)
4) run generated unloading jobs on source account
5) run genrated loading jobs on target account
 
Step 12: Right before frozen:
Generating statement to disable user or suspend warehouse Handling how to freeze source account 
    python main.py -c 2 -w -dis 
    (adding excluding lists for user and warehouse ??? not yet )

  The following files are generated (send over to customers):
    42_suspend_warehouses.sql
    42_resume_warehouses.sql
    43_disable_users.sql
    43_enable_users.sql 

Step 13: Freeze Primary account so there is no changes against primary account
- by disable users or suspend warehouse using above files (from step 12)

In the same time while customer primary account is frozen, re-run the following in PROD 
python main.py -c all -b -o WAREHOUSE RESOURCE_MONITOR -r nopwd -p -d all -stage -pipe -f

  to get incremental updates of metadata (-c)
  to Generate account grants (-b)
  to Generate account-level objects and their grants of ownership and privileges (-o)
  to Generate users/roles and their relationships (-r)
  to Generate account parameters (-p)
  to generate all grants for database level objects (-d)
  to generate stage DDL and their grants (-stage)
  to generate pipe DDL and their grants (-pipe)
  to generate future grants (-f)

The following files are generated (send over to customers):
  11_creater_monitors_DDL.sql
  12_create_network_policies_DDL.sql
  13_create_warehouses_DDL.sql
  14_grant_acct_level_privs.sql
  21_create_users.sql
  22_create_roles.sql
  23_grant_roles.sql
  24_grant_target_roles.sql
  25_grant_owner_dblevel(_xxx).sql
  26_grant_privs_dblevel(_xxx).sql
  27_future_grants.sql
  31_create_stages_DDL.sql
  32_create_pipes_DDL.sql
  41_set_parameters.sql

  Note: Generating users/roles and their relationships 
  python main.py -r nopwd     => users without pwd
  python main.py -r samepwd   => users with dame pwd
  python main.py -r randpwd   => users with random pwd

Step 13: Ask Customer to execute sql commands of refresh standby databases on customer's stand-by accounts - final refresh
snowsql -f 03_refresh_all_global_xxx.sql

Step 14: Ask Customer to execute sql commands of monitor progress of refreshing on customer's stand-by accounts periodically till refreshing is complete
snowsql -f 04_monitor_last_refresh_xxx.sql

Step 15: Ask Customer to Switch over standby database to primary for failover:
snowsql -f 05_switchover_to_standby_xxx.sql

or to run the following on both source and target account to detach from global databases so all databases become local.
snowsql -f b1_alter_failover_dbs.sql

if you want to rerun the whole process, you can generate all of the drop statements using option -t (with an excluding list) and run them on target accounts 
python main.py -t ADMIN KHOYLE MYANG DBRYANT MBROWN

The following files are generated :
  x0_drop_acctobj.sql
  x1_drop_dbobj.sql
  x2_drop_roles.sql

Post-replication actions:
1. Run the following jobs on target account :
  11_creater_monitors_DDL.sql
  12_create_network_policies_DDL.sql
  13_create_warehouses_DDL.sql
  14_grant_acct_level_privs.sql
  21_create_users.sql
  22_create_roles.sql
  23_grant_roles.sql
  24_grant_target_roles.sql
  25_grant_owner_dblevel(_xxx).sql
  26_grant_privs_dblevel(_xxx).sql
  27_future_grants.sql
  31_create_stages_DDL.sql
  32_create_pipes_DDL.sql
  41_set_parameters.sql

 2.  Run the following jobs on source and target account :
  a1_add_fk_xxx.sql
  a2_extb_DDL_xxx.sql
  a3_crossdb_MVDDL_xxx.sql

3. Validating
  - auto-clustering
  - materilaized views
  - row counts & hash_agg
  - account_usage 
  - run some sample workload and queries 
  - user login

4. drop role repadmin 


Typical use cases (using CUSTOMER mode as example):
1. using snowflake replication engine, migrating an account from one region to another region (CUSTOMER mode)
step 1: generate scripts
python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile -f -u 

dbfile is the file name that contains database list in the same folder as this python code parent folder. Command above will generate DDLs
for account level objects and grants for all objects specicified.

step 2: run replication steps
python main.py -g dbfile
python main.py -l dbfile

step 3: lastly run all generated scripts in step 1 above.


2. migrating an account's account level metadata only, no data replication
python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile -f

Then run all generated scripts in above command .

2. migrating an account from one region to another region or from one cloud to another cloud using manual replication instead of snowflake replication engine
step 1: generate scripts
python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile  -f -u

dbfile is the file name that contains database list in the same folder as this python code parent folder. Command above will generate DDLs
for account level objects and grants for all objects specicified.

step 2: run manual replication steps
python main.py -ddl dbfile
python main.py -elt dbfile

Run generated DDLs and ELT jobs.

step 3: lastly run all generated scripts in step 1 above.

4. You want to get everything including users, roles, privileges, account_level objects, DDL/ELT jobs for manual replicaiton, and replication commands from snowflake replicaiton engine.
  python main.py -c all -d all -r nopwd -g all -l all -stage all -pipe all -b -f -o WAREHOUSE NETWORK_POLICY RESOURCE_MONITOR 

Note: 
Integration can only be geneated from SNOWHOUSE (snowflake internally) due to SHOW INTEGRATIONS not providing enough information for the DDLs.

Output script structure (if not existing in your loacal folder, you should create them ahead of time):
scripts
  acctobj   ==> account level objects and its grants 
  ddl       ==> database level object DDLs  
  elt       ==> unloading and loading jobs for tables (manual replication)
  eval      ==> script for evaluation including evaluating dependency for replicaiton and validation after replication
  rbac      ==> grants for database level objects
  rep       ==> scripts for replication commands (using snowflake replicaiton engine)
  snowhouse ==> scripts generated on snowhouse including INTEGRATION DDL, dependency report on snowhouse database 
