#!/bin/bash

cwd=`dirname $0`;
echo $cwd;
source $cwd/env_xxx.sh;
cd $MIGRATION_HOME;
echo "source account: $SRC_CUST_ACCOUNT";
echo "target account: $TGT_CUST_ACCOUNT";
now=$(date -u "+%y%m%d_%H%M%S");
# step 1: generate ddl sql scripts by reading metadata from source account
echo "Started generate_nonreplicated_objects_ddl script at: $now" >> logs/generate_nonreplicated_objects_ddl_$now.log ; 
python3 -u run_nonreplicated_objects.py -d dbfile >> logs/generate_nonreplicated_objects_ddl_$now.log;
endtime=$(date -u "+%y%m%d_%H%M%S");
echo "Finished generate_nonreplicated_objects_ddl script at: $endtime" >> logs/generate_nonreplicated_objects_ddl_$now.log;
