#!/bin/bash

cd /EDW1/appl/spool/users/ed07dw/replication/itds_dev_east/migration_home/;

source env_xxx.sh
echo "source account: $SRC_CUST_ACCOUNT";
echo "target account: $TGT_CUST_ACCOUNT";
now=$(date -u "+%y%m%d_%H%M%S");
echo "started run_nonreplicated_objects script at: $now";
echo "$now" > logs/run_nonreplicated_objects_$now.log
python3 -u run_nonreplicated_objects.py -d dbfile >> logs/run_nonreplicated_objects_$now.log
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "$endtime" >> logs/run_nonreplicated_objects_$now.log;
echo "finished run_nonreplicated_objects script at: $endtime";

export TGT_CUST_ROLE=ACCOUNTADMIN
export TARGET_ROLE=ACCOUNTADMIN
export FAILED_QUERIES_LOG_FILE=sstp_stages_failed_queries.log
now=$(date -u "+%y%m%d_%H%M%S");
echo "started create_nonreplicated_objects script at: $now";
echo "$now" > logs/sstp_nohup_$now.log
python3 -u main.py -m DR -scriptslistfile ddl_scripts_to_run >> logs/sstp_nohup_$now.log
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "$endtime" >> logs/sstp_nohup_$now.log
echo "completed create_nonreplicated_objects script at: $endtime";
exit;

now=$(date -u "+%y%m%d_%H%M%S");
echo "started grenerate_cross_rep_scripts at: $now";
echo "$now" > logs/generate_crossrep_scripts_$now.log
#python3 -u run_crossrep_noroles.py >> logs/generate_crossrep_scripts_$now.log
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "$endtime" >> logs/generate_crossrep_scripts_$now.log
echo "finished run_crossrep_noroles script at: $endtime";
exit;

export TGT_CUST_ROLE=ACCOUNTADMIN
export TARGET_ROLE=ACCOUNTADMIN
export FAILED_QUERIES_LOG_FILE=acctobj_failed_queries.log
now=$(date -u "+%y%m%d_%H%M%S");
echo "started acctobjscripts at: $now";
echo "$now" > logs/acctobj_nohup_$now.log
#python3 -u main.py -m DR -scriptslistfile acctobj_scriptstorunfile >> logs/acctobj_nohup_$now.log
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "$endtime" >> logs/acctobj_nohup_$now.log

export TGT_CUST_ROLE=SECURITYADMIN
export TARGET_ROLE=SECURITYADMIN
export FAILED_QUERIES_LOG_FILE=rbac_failed_queries.log
now=$(date -u "+%y%m%d_%H%M%S");
echo "started rbacscripts at: $now";
echo "$now" > logs/rbac_nohup_$now.log
#python3 -u main.py -m DR -scriptslistfile rbac_scriptstorunfile -splitbylinesfile splitfiles_list -parallel 64 >> logs/rbac_nohup_$now.log
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "$endtime" >> logs/rbac_nohup_$now.log
