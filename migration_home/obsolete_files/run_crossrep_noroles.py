#!/usr/bin/env python3
import os, subprocess, sys # import snowflake.connector
from datetime import datetime

""" USAGE: python3 run_crossrep.py 
        # def with_engine() :   return "python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile -f"
        # def metadata_only() : return "python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile -f"
        # def manual() :        return "python main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR NETWORK_POLICY -r nopwd -p -d dbfile -stage dbfile -pipe dbfile -f -u"
        # def full() :          return "python main.py -c all -d all -r nopwd -g all -l all -stage all -pipe all -b -f -o WAREHOUSE NETWORK_POLICY RESOURCE_MONITOR"
"""
DEBUG = False
PURGE_SQL = False #True
INIT = "python main.py -m CUSTOMER -c all"
CROSS_REP = "python3 main.py -m CUSTOMER -c all -b -o WAREHOUSE RESOURCE_MONITOR -pipe dbfile"

def migrate(case, debug=False) :
    if debug : print(case)
    else : subprocess.call(case, shell=True)
    return

# ------------------------------------------------------------------------------------------------------------------------------------------------
# Main routine...
# ------------------------------------------------------------------------------------------------------------------------------------------------
print(f"started script run_crossrep.py : {datetime.now()}")
#migrate(INIT, DEBUG)
#run_migration_results(PURGE_SQL, DEBUG)
migrate(CROSS_REP, DEBUG)
#run_migration_results(PURGE_SQL, DEBUG)
print(f"finished script run_crossrep.py : {datetime.now()}")
sys.exit()
