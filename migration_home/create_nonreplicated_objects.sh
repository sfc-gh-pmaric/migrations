#!/bin/bash

cwd=`dirname $0`;
echo $cwd;
source $cwd/env_xxx.sh;
cd $MIGRATION_HOME;
echo "source account: $SRC_CUST_ACCOUNT";
echo "target account: $TGT_CUST_ACCOUNT";
export TGT_CUST_ROLE=ACCOUNTADMIN
export TARGET_ROLE=ACCOUNTADMIN
export FAILED_QUERIES_LOG_FILE=sstp_stages_failed_queries.log
now=$(date -u "+%y%m%d_%H%M%S");
echo "Started create_nonreplicated_objects script at: $now" >> logs/create_nonreplicated_objects_$now.log ;
python3 -u main.py -m DR -scriptslistfile ddl_scripts_to_run >> logs/create_nonreplicated_objects_$now.log;
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "Finished generate_nonreplicated_objects_ddl script at: $endtime" >> logs/create_nonreplicated_objects_$now.log;
