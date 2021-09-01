#!/bin/bash

source env_xxx.sh
export TGT_CUST_ROLE=ACCOUNTADMIN
export TARGET_ROLE=ACCOUNTADMIN
export FAILED_QUERIES_LOG_FILE=acctobj_failed_queries.log
echo $SRC_CUST_ACCOUNT;
now=$(date -u "+%y%m%d_%H%M%S");
echo "$now";

python3 -u main.py -m DR -scriptslistfile acctobj_scriptstorunfile > logs/acctobj_nohup_$now.log
