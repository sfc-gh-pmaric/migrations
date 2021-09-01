#!/bin/bash

source env_xxx.sh
export TGT_CUST_ROLE=SECURITYADMIN
export TARGET_ROLE=SECURITYADMIN 
export FAILED_QUERIES_LOG_FILE=rbac_failed_queries.log
echo $SRC_CUST_ACCOUNT;
now=$(date -u "+%y%m%d_%H%M%S");
echo "$now" > logs/rbac_nohup_$now.log
python3 -u main.py -m DR -scriptslistfile rbac_scriptstorunfile -splitbylinesfile splitfiles_list -parallel 1 >> logs/rbac_nohup_$now.log
echo "$now" > logs/rbac_nohup_$now.log
