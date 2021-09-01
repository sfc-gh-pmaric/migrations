import snowflake.connector
import argparse 
#import pandas as pd
import os 
import re
from snowflake.connector import DictCursor

def isBlank(var):
    if var == None :
        return True
    elif var.strip() == '':
        return True
    else:
        return False

def readFile(filename):
    alist = []
    with open( filename,"r") as df:
        for db in df:
            db = db.strip()
            if isBlank(db) == False:
                alist.append(db)
    df.close()
    return alist

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
    if table_name.count('.') > 1:
    	this_db_name = table_name.split('.')[0]
    	this_schema_name = table_name.split('.')[1]
    elif table_name.count('.') > 0:
    	this_db_name = db_name 
    	this_schema_name = table_name.split('.')[0] 
    else:
    	this_db_name = db_name 
    	this_schema_name = schema_name 
        
    res = cur.execute(f"""
        select count(*) as record_count
        from {this_db_name}.information_schema.tables 
        where table_schema = '{this_schema_name}'
        and concat(table_catalog, '.', table_schema, '.', table_name) = '{l_table_name}'  
    """)
    rc = cur.fetchone()['RECORD_COUNT']
    return rc

def get_grants(cur, object_name, object_type):
    grants_array = {"ownership":[], "other":[]}
    if object_type == 'SHARE':
        grant_type_list = ['ON', 'TO']
    else:
        grant_type_list = ['ON']
    for gt in grant_type_list:
        try:  
            res = cur.execute(f"show grants {gt} {object_type} {object_name}")
            for rec in cur:
                grant_stmt = '' 
                if rec['grant_option'] == 'true':
                    grant_option = 'with grant option'
                else:
                    grant_option = ''
                if rec['privilege'] == 'OWNERSHIP':
                    grant_stmt = f"""grant {rec['privilege']} on {rec['granted_on']} {rec['name']} to {rec['granted_to']} {rec['grantee_name']} copy current grants;"""
                else: 
                    grant_stmt = f"""grant {rec['privilege']} on {rec['granted_on']} {rec['name']} to {rec['granted_to']} {rec['grantee_name']} {grant_option} ;"""
                if grant_stmt: 
                    if rec['privilege'] == 'OWNERSHIP':
                        grants_array['ownership'].append(grant_stmt)
                    else:
                        grants_array['other'].append(grant_stmt)
                #print(grant_stmt)
        except:
            pass 
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
    stmt = f"""{create_clause} {object_name} {on_clause} {rec['table_name']} {mode} COMMENT = '{rec['comment']}';"""
    return stmt

def get_stage_details(cur, qid):
    sql_text = f"""
        select 
            * 
        from table(result_scan('{qid}'))
        where length(trim("property_value")) > 0 
        and "property" not in ('URL', 'STORAGE_INTEGRATION') 
        order by 
            case 
                when "parent_property" = 'STAGE_LOCATION' then 0 
                when "parent_property" = 'STAGE_FILE_FORMAT' then 1
                when "parent_property" = 'STAGE_COPY_OPTIONS'then 2 else 99 end, 
            case when "property" = 'TYPE' then 0 else 99 end, "property"
    """
    res = cur.execute(sql_text)
    gen_ddl = ''
    curr_parent_property = '' 
    for rec in cur:
        if rec['parent_property'] != curr_parent_property:
            if curr_parent_property:
                if curr_parent_property in ('STAGE_FILE_FORMAT', 'STAGE_COPY_OPTIONS'):
                    gen_ddl += ' \n)'
            curr_parent_property = rec['parent_property']
            if curr_parent_property in ('STAGE_FILE_FORMAT', 'STAGE_COPY_OPTIONS'):
                    gen_ddl += f""" \n  {curr_parent_property.replace('STAGE_', '')} = ( """
        if rec['property_type'] == 'String':
            gen_ddl += f"""\n  {rec['property']} = '{rec['property_value']}' """
        elif rec['property_type'] == 'List':
            prop_value_arr = re.sub(r'[\[|\]]','',rec['property_value']).split(',')
            #print(prop_value_arr)
            prop_value_str = '(' + ','.join(["'" + x.strip().replace('\\\\\\','\\\\') + "'" for x in prop_value_arr]) + ')'
            #print(prop_value_str)
            gen_ddl += f"""\n {rec['property']} = {prop_value_str} """ 
        else:
            gen_ddl += f"""\n  {rec['property']} = {rec['property_value']} """
    if curr_parent_property in ('STAGE_FILE_FORMAT', 'STAGE_COPY_OPTIONS'):
        gen_ddl += ' \n)'
    #print(gen_ddl)                        
    return gen_ddl

def get_stage_ddl(cur, stage_record):
    #print(stage_record)
    stmt = ''
    url = '' 
    creds_or_integration = '' 
    encryption = '' 
    rec = stage_record 
    object_name = '"' + rec['database_name'] + '"."' + rec['schema_name'] + '"."' + rec['name']+'"'
    create_clause = 'CREATE OR REPLACE '
    if 'TEMPORARY' in rec['type']:
        create_clause += ' TEMPORARY STAGE'
    else:
        create_clause += ' STAGE'
    if rec['url']:
        url = f"URL='{rec['url']}'"
    if rec['storage_integration']:
        creds_or_integration = f"STORAGE_INTEGRATION = {rec['storage_integration']}"
    elif rec['cloud'] == 'AZURE':
        if rec['has_credentials'] == 'Y':
            creds_or_integration = f"CREDENTIALS = ( AZURE_SAS_TOKEN = '$azure_sas_token' )"
    if rec['has_encryption_key'] == 'Y' and rec['cloud'] == 'AZURE':
        encryption = f"ENCRYPTION = (TYPE = 'AZURE_CSE' MASTER_KEY = '$azure_encryption_key')" 
    try:
        if (rec['type'] == 'INTERNAL TEMPORARY' or rec['database_name'] == 'WORKSHEETS_APP' 
            or (rec['type'] == 'EXTERNAL' and (not rec['storage_integration'])) ) :
            stmt = ''
        else: 
            desc_sql_text = f"DESCRIBE STAGE {object_name}"
            #print(desc_sql_text)
            res = cur.execute(desc_sql_text)
            qid = cur.sfqid 
            options = get_stage_details(cur, qid)
            stmt = f"""{create_clause} {object_name} {url} {creds_or_integration} {encryption} {options} COMMENT = '{rec['comment']}';"""
    except Exception as e:
        print(e)
    return stmt

def get_share_ddl(cur, rec):
    ddl_array = [] 
    object_name = rec['name'] 
    if rec['kind'] == 'OUTBOUND': 
        cr_stmt = f"CREATE SHARE IF NOT EXISTS {object_name} ;"
        ddl_array.append(cr_stmt)
        if rec['database_name']:
            gr_stmt = f"GRANT USAGE ON DATABASE {rec['database_name']} TO SHARE {object_name} ;"
            ddl_array.append(gr_stmt)
        if rec['to']:
            alter_stmt = f"ALTER SHARE {object_name} add accounts={rec['to']} ;"
            ddl_array.append(alter_stmt)
    return ddl_array

def get_ddl_and_grants(cur, input_array, ddl_array, grant_array, object_type='STREAM'):
    curr_db = ''
    curr_schema = ''
    for rec in input_array:
        use_stmt_array = []
        share_ddl_array = [] 
        object_name = '' 
        ddl_stmt = '' 
        if curr_db != rec['database_name']:
            curr_db = rec['database_name']
            if curr_db: use_stmt_array.append(f"USE DATABASE {curr_db}; ")
        if curr_schema != rec.get('schema_name',''):
            curr_schema = rec.get('schema_name','')
            if curr_schema: use_stmt_array.append(f"USE SCHEMA {curr_schema}; ")
        if rec.get('schema_name','') and rec.get('database_name'): 
            object_name = rec['database_name']+'.'+rec['schema_name']+'."'+rec['name']+'"'
        if object_type == 'STREAM':
            ddl_stmt = get_stream_ddl(rec)
        elif object_type == 'STAGE':
            #print('before get_stage_ddl')
            ddl_stmt = get_stage_ddl(cur, rec)
            #print('after get_stage_ddl')
        elif object_type == 'SHARE':
            object_name = rec['name']
            share_ddl_array = get_share_ddl(cur, rec)
            if len(share_ddl_array) > 0:
                ddl_stmt = share_ddl_array[0]
        else:
            res = cur.execute(f"select get_ddl('{object_type}','{object_name}') as ddl_stmt")
            ddl_stmt = cur.fetchone()['DDL_STMT']
        #print(ddl_stmt)
        if len(use_stmt_array) > 0:
            ddl_array.extend(use_stmt_array)
        if ddl_stmt:
            grants_for_object = get_grants(cur, object_name, object_type)
        if object_type == 'STREAM' and ddl_stmt: 
            #print(f"table_name is : {rec['table_name']}")
            # check if object exists or not 
            if table_exists(cur, curr_db, curr_schema, rec['table_name']) > 0:
                #ddl_array.extend(use_stmt_array)
                ddl_array.append(ddl_stmt)
                grant_array.append(grants_for_object)
        elif ddl_stmt: 
            #ddl_array.extend(use_stmt_array)
            if object_type == 'SHARE':
                ddl_array.extend(share_ddl_array)
            else:
                ddl_array.append(ddl_stmt)
            if object_type == 'PIPE':
                alter_pipe_stmt = f"ALTER PIPE {object_name} SET PIPE_EXECUTION_PAUSED=TRUE; "
                ddl_array.append(alter_pipe_stmt)
            grant_array.append(grants_for_object)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate DDL for Stages, Pipes, Streams, Tasks and Inbound Shares',
        epilog='Example: python3 run_nonreplicated_objects.py -d dbfile')
    parser.add_argument('-d', '--dbfile',  type=str, help='a file name with database lists to grant or all databases (all) or nothing ')
    args=parser.parse_args()
    dblist = [] 
    #dblist = ['EDM_CONFIRMED_OUT_DEV']
    if args.dbfile: 
        dbfile = os.path.join(os.environ.get('MIGRATION_HOME'), args.dbfile)
        dblist = readFile(dbfile)

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
    out_directory = os.path.join(os.environ.get('MIGRATION_HOME'), 'scripts', 'ddl')
    streams_array = []
    tasks_array = []
    # ddl_array = []
    stages_array = []
    object_dict = {'STAGE':[],'STREAM':[],'TASK':[],'SHARE':[],'PIPE':[]}
    grants_dict = {'STAGE':[],'STREAM':[],'TASK':[],'SHARE':[],'PIPE':[]}
    # object_dict = {'STAGE':[]}
    # grants_dict = {'STAGE':[]}
    # object_dict = {'SHARE':[]}
    out_file_dict = {
        'STAGE': '33_create_stages.sql',
        'STREAM': '34_create_streams.sql',
        'TASK': '35_create_tasks.sql',
        'SHARE': '36_create_shares.sql',
        'PIPE': '37_create_pipes.sql'
        }
    grant_file_dict = {
        'STAGE': {
            'OWNERSHIP': '51_grant_stage_ownership.sql',
            'OTHER': '52_grant_stage_privileges.sql'
        },
        'STREAM': {
            'OWNERSHIP': '53_grant_stream_ownership.sql',
            'OTHER': '54_grant_stream_privileges.sql'
        },
        'TASK': {
            'OWNERSHIP': '55_grant_task_ownership.sql',
            'OTHER': '56_grant_task_privileges.sql'
        },
        'PIPE': {
            'OWNERSHIP': '57_grant_pipe_ownership.sql',
            'OTHER': '58_grant_pipe_privileges.sql'
        },
        'SHARE': {
            'OWNERSHIP': '59_grant_share_ownership.sql',
            'OTHER': '60_grant_share_privileges.sql'
        }
    }

    if len(dblist) == 0:
        for k in object_dict:
            object_dict[k] = get_objects_for_account(cur, k)
    else:
        for k in object_dict:
            if k == 'SHARE':
                object_dict[k] = get_objects_for_account(cur, k)
            else:
                object_dict[k] = get_objects_by_db(cur, dblist, k)
    for k in object_dict:
        ddl_array = [] 
        grant_array = [] 
        if len(object_dict[k]) > 0:
            get_ddl_and_grants(cur, object_dict[k], ddl_array, grant_array, k)
            if len(ddl_array) > 0:
                write_to_file(out_directory,out_file_dict[k],ddl_array)
        if len(grant_array) > 0:
            grants_dict[k] = grant_array
   
    for ot in grants_dict:
        ownership_array = []
        for d in grants_dict[ot]:
            ownership_array.extend(d['ownership'])
        if len(ownership_array) > 0:
            write_to_file(out_directory,grant_file_dict[ot]['OWNERSHIP'],ownership_array)
        privs_array = []
        for d in grants_dict[ot]:
            privs_array.extend(d['other'])
        if len(privs_array) > 0:
            write_to_file(out_directory,grant_file_dict[ot]['OTHER'],privs_array)
    con.close() # close the connection

'''
import datetime
username = 'rsree01@safeway.com'
curtime = datetime.datetime.now().strftime('%Y%m%d%H%M%s')
genpass = hashlib.md5((username+str(curtime)+'abscrossrep').encode('utf8')).hexdigest()
'''
