#!/bin/bash

source env_xxx.sh
echo $SRC_CUST_ACCOUNT;
now=$(date -u "+%y%m%d_%H%M%S");
echo "$now";

python3 -u run_crossrep.py > logs/generate_crossrep_scripts_$now.log
