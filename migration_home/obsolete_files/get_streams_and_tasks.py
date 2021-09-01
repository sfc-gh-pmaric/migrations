import snowflake.connector
import pandas as pd
import os 
from snowflake.connector import DictCursor

def write_to_file(dir_name,file_name,input_array):
    with open(os.path.join(dir_name, file_name), "w") as f:
        for line in input_array:
            f.write(line)
            f.write('\n')
            
def get_objects_by_db(cur, dblist, object_type):
    obj_array = []
    for db in dblist:
        stmt = f"show {object_type}s in database {db}"
        res = cur.execute(stmt)
        for rec in cur:
            obj_array.append(rec)
    return obj_array

def get_objects_for_account(cur, object_type):
    obj_array = []
    stmt = f"show {object_type}s in account"
    res = cur.execute(stmt)
    for rec in cur:
        obj_array.append(rec)
    return obj_array

def table_exists(cur, db_name, schema_name, table_name):
    rc = 0 
    if table_name.find('"') > 0:
        l_table_name = table_name.replace('"','')
    else:
        l_table_name = table_name
    res = cur.execute(f"""
        select count(*) as record_count
        from {db_name}.information_schema.tables 
        where table_schema = '{schema_name}'
        and concat(table_catalog, '.', table_schema, '.', table_name) = '{l_table_name}'  
    """)
    rc = cur.fetchone()['RECORD_COUNT']
    return rc

def get_grants(cur, object_name, object_type):
    grants_array = {"ownership":[], "other":[]}
    res = cur.execute(f"show grants on {object_type} {object_name}")
    for rec in cur:
        grant_stmt = '' 
        if rec['grant_option'] == 'true':
            grant_option = 'with grant option'
        else:
            grant_option = ''
        grant_stmt = f"""grant {rec['privilege']} 
        on {rec['granted_on']} {rec['name']} to {rec['granted_to']} {rec['grantee_name']} {grant_option} ;
        """
        if grant_stmt: 
            if rec['privilege'] == 'OWNERSHIP':
                grants_array['ownership'].append(grant_stmt)
            else:
                grants_array['other'].append(grant_stmt)
        print(grant_stmt)
    return grants_array

def get_stream_ddl(stream_record):
    stmt = ''
    rec = stream_record
    object_name = rec['database_name']+'.'+rec['schema_name']+'."'+rec['name']+'"'
    create_clause = 'CREATE OR REPLACE STREAM'
    on_clause = 'ON TABLE'
    if rec['mode'] == 'APPEND_ONLY':
        mode = 'APPEND_ONLY = TRUE'
    elif rec['mode'] == 'INSERT_ONLY':
        mode = 'INSERT_ONLY = TRUE'
        on_clause = 'ON EXTERNAL TABLE'
    else:
        mode = ''
    # log a bug for copy grants - it does not work 
    stmt = f"""
        {create_clause} {object_name} 
        {on_clause} {rec['table_name']} 
        {mode} 
        COMMENT = '{rec['comment']}';
        """
    return stmt

def get_ddl_and_grants(cur, input_array, ddl_array, grant_array, object_type='STREAM'):
    curr_db = ''
    curr_schema = ''
    for rec in input_array:
        use_stmt_array = []
        if curr_db != rec['database_name']:
            curr_db = rec['database_name']
            if curr_db: use_stmt_array.append(f"USE DATABASE {curr_db}; ")
        if curr_schema != rec['schema_name']:
            curr_schema = rec['schema_name']
            if curr_schema: use_stmt_array.append(f"USE SCHEMA {curr_schema}; ")
        object_name = rec['database_name']+'.'+rec['schema_name']+'."'+rec['name']+'"'
        if object_type == 'STREAM':
            ddl_stmt = get_stream_ddl(rec)
        else:
            res = cur.execute(f"select get_ddl('{object_type}','{object_name}') as ddl_stmt")
            ddl_stmt = cur.fetchone()['DDL_STMT']
        print(ddl_stmt)
        grants_for_object = get_grants(cur, object_name, object_type)
        if object_type == 'STREAM':
            print(f"table_name is : {rec['table_name']}")
            # check if object exists or not 
            if table_exists(cur, curr_db, curr_schema, rec['table_name']) > 0:
                ddl_array.extend(use_stmt_array)
                ddl_array.append(ddl_stmt)
                grant_array.append(grants_for_object)
        else:
            ddl_array.extend(use_stmt_array)
            ddl_array.append(ddl_stmt)
            grant_array.append(grants_for_object)


if __name__ == '__main__':
    con = snowflake.connector.connect(
        user=os.environ.get('SRC_CUST_USER'),
        password=os.environ.get('SRC_CUST_PWD'),
        account=os.environ.get('SRC_CUST_ACCOUNT'),
        database=os.environ.get('SRC_CUST_DATABASE'),
        warehouse=os.environ.get('SRC_CUST_WAREHOUSE'),
        role=os.environ.get('SRC_CUST_ROLE')
    )
    cur = con.cursor(DictCursor)
    cur.execute("use role accountadmin")
    with open(os.path.join(os.environ.get('MIGRATION_HOME'), 'dbfile'), "r") as f:
        dblist = f.readlines()
    print(dblist)
    out_directory = os.path.join(os.environ.get('MIGRATION_HOME'), 'scripts', 'ddl')
    streams_array = []
    tasks_array = []
    ddl_array = []
    grant_array = []
    if len(dblist) == 0:
        streams_array = get_objects_for_account(cur, 'STREAM')
        tasks_array = get_objects_for_account(cur, 'TASK')
    else:
        streams_array = get_objects_by_db(cur, dblist, 'STREAM')
        tasks_array = get_objects_by_db(cur, dblist, 'TASK')
    if len(streams_array) > 0:
        get_ddl_and_grants(cur, streams_array, ddl_array, grant_array, 'STREAM')
    if len(tasks_array) > 0:
        get_ddl_and_grants(cur, tasks_array, ddl_array, grant_array, 'TASK')

    # write out the ddl
    out_file_name = "33_streams_and_tasks.sql"
    write_to_file(out_directory,out_file_name,ddl_array)
    # write out the grants
    #print(grant_array)
    out_file_name = "34_grant_ownership_for_streams_and_tasks.sql"
    ownership_array = []
    for d in grant_array:
        ownership_array.extend(d['ownership'])
    write_to_file(out_directory,out_file_name,ownership_array)
    out_file_name = "35_grant_privs_for_streams_and_tasks.sql"
    privs_array = []
    for d in grant_array:
        privs_array.extend(d['other'])
    write_to_file(out_directory,out_file_name,privs_array)
    con.close() # close the connection

