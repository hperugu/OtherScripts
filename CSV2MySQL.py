# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 14:36:44 2019

@author: hperugu
"""

import numpy as np
import six
import pandas.io.sql as psql
import pandas as pd
import pdb
import objgraph

#from dateutil import parser

dbtypes={
    'mysql' : {'DATE':'DATE', 'DATETIME':'DATETIME',           'INT':'BIGINT',  'FLOAT':'FLOAT',  'VARCHAR':'VARCHAR'},
    'oracle': {'DATE':'DATE', 'DATETIME':'DATE',               'INT':'NUMBER',  'FLOAT':'NUMBER', 'VARCHAR':'VARCHAR2'},
    'sqlite': {'DATE':'TIMESTAMP', 'DATETIME':'TIMESTAMP',     'INT':'NUMBER',  'FLOAT':'NUMBER', 'VARCHAR':'VARCHAR2'},
    'postgresql': {'DATE':'TIMESTAMP', 'DATETIME':'TIMESTAMP', 'INT':'BIGINT',  'FLOAT':'REAL',   'VARCHAR':'TEXT'},
}


def read_db(sql, con):
    return psql.read_sql_query(sql, con)

def table_exists(name=None, con=None, flavor='sqlite'):
    if flavor == 'sqlite':
        sql="SELECT name FROM sqlite_master WHERE type='table' AND name='MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'mysql':
        sql="show tables like 'MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'postgresql':
        sql= "SELECT * FROM pg_tables WHERE tablename='MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'oracle':
        sql="select table_name from user_tables where table_name='MYTABLE'".replace('MYTABLE', name.upper())
    elif flavor == 'odbc':
        raise NotImplementedError
    else:
        raise NotImplementedError
    
    df = read_db(sql, con)
    #print (sql, df)
    print ('table_exists?', len(df))
    exists = True if len(df)>0 else False
    return exists

def write_frame(frame, name=None, con=None, flavor='sqlite', if_exists='fail'):
    """
    Write records stored in a DataFrame to specified dbms. 
    
    if_exists:
        'fail'    - create table will be attempted and fail
        'replace' - if table with 'name' exists, it will be deleted        
        'append'  - assume table with correct schema exists and add data.  if no table or bad data, then fail.
            ??? if table doesn't exist, make it.
        if table already exists.  Add: if_exists=('replace','append','fail')
    """

    if if_exists=='replace' and table_exists(name, con, flavor):    
        cur = con.cursor()   
        cur.execute("drop table "+name)
        cur.close()    
    
    if if_exists in ('fail','replace') or ( if_exists=='append' and table_exists(name, con, flavor)==False ):
        #create table
        schema = get_schema(frame, name, flavor)
        if flavor=='oracle':
            schema = schema.replace(';','')
        cur = con.cursor()    
        if flavor=='mysql':
            cur.execute("SET sql_mode='ANSI_QUOTES';")
        print ('schema\n', schema)
        cur.execute(schema)
        cur.close()
        print ('created table') 
        
    cur = con.cursor()
    #bulk insert
    if flavor=='sqlite' or flavor=='odbc':       
        wildcards = ','.join(['?'] * len(frame.columns))
        insert_sql = 'INSERT INTO %s VALUES (%s)' % (name, wildcards)
        #print 'insert_sql', insert_sql
        data = [tuple(x) for x in frame.values]
        #print 'data', data
        cur.executemany(insert_sql, data)
        

    elif flavor=='mysql':
        
        wildcards = ','.join(['%s'] * len(frame.columns))
        cols=[db_colname(k) for k in frame.dtypes.index]
        colnames = ','.join(cols)
        insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (name, colnames, wildcards)
        #print (insert_sql)
        #data = [tuple(x) for x in frame.values]
        frame = timestamp2string(frame)
        data = nan2none(frame)
        #data= [ tuple([ None if pd.isnull(v) else v for v in rw]) for rw in frame.values ]
        #print (data[0])
        cur.executemany(insert_sql, data)
        
    elif flavor=='postgresql':
        postgresql_copy_from(frame, name, con)    
    else:
        raise NotImplementedError        
    con.commit()
    cur.close()
    return

def nan2none(df):
    dnp = df.values
        
    tpl_list= [ tuple([ None if pd.isnull(v) else v for v in rw]) for rw in dnp ] 
    
    return tpl_list

def timestamp2string(df):
    for col in df:
        if df[col].dtype.kind == 'M':            
            df[col] = df[col].dt.strftime('%Y/%m/%d')
    return df

def db_colname(pandas_colname):
    '''convert pandas column name to a DBMS column name'''
    ''' TODO: deal with name length restrictions, esp for Oracle    '''
    colname =  pandas_colname.replace(' ','_').strip()                  
    return colname


def postgresql_copy_from(df, name, con ):
    # append data into existing postgresql table using COPY
    
    # 1. convert df to csv no header
    output = six.StringIO()
    
    # deal with dateticStringIOme64 to_csv() bug
    have_datetime64 = False
    dtypes = df.dtypes
    for i, k in enumerate(dtypes.index):
        dt = dtypes[k]
        #print ('dtype', dt, dt.itemsize)
        if str(dt.type)=="<type 'numpy.datetime64'>":
            have_datetime64 = True

    if have_datetime64:
        d2=df.copy()    
        for i, k in enumerate(dtypes.index):
            dt = dtypes[k]
            if str(dt.type)=="<type 'numpy.datetime64'>":
                d2[k] = [ v.to_pydatetime() for v in d2[k] ]                
        #convert datetime64 to datetime
        #ddt= [v.to_pydatetime() for v in dd] #convert datetime64 to datetime
        d2.to_csv(output, sep='\t', header=False, index=False)
    else:
        df.to_csv(output, sep='\t', header=False, index=False)                        
    output.seek(0)
    contents = output.getvalue()
    #print ('contents\n', contents)
       
    # 2. copy from
    cur = con.cursor()
    cur.copy_from(output, name)    
    con.commit()
    cur.close()
    return

def get_schema(frame, name, flavor):
    types = dbtypes[flavor]  #deal with datatype differences
    column_types = []
    dtypes = frame.dtypes
    for i,k in enumerate(dtypes.index):
        dt = dtypes[k]
        #print 'dtype', dt, dt.itemsize
        if str(dt.type)=="<type 'numpy.datetime64'>":
            sqltype = types['DATETIME']
        elif issubclass(dt.type, np.datetime64):
            sqltype = types['DATETIME']
        elif issubclass(dt.type, (np.integer, np.bool_)):
            sqltype = types['INT']
        elif issubclass(dt.type, np.floating):
            sqltype = types['FLOAT']
        else:
            sampl = frame[ frame.columns[i] ][0]
            #print 'other', type(sampl)    
            if str(type(sampl))=="<type 'datetime.datetime'>":
                sqltype = types['DATETIME']
            elif str(type(sampl))=="<type 'datetime.date'>":
                sqltype = types['DATE']                   
            else:
                if flavor in ('mysql','oracle'):                
                    size = 2 + max( (len(str(a)) for a in frame[k]) )
                    print (k,'varchar sz', size)
                    sqltype = types['VARCHAR'] + '(?)'.replace('?', str(size) )
                else:
                    sqltype = types['VARCHAR']
        colname =  db_colname(k)  #k.upper().replace(' ','_')                  
        column_types.append((colname, sqltype))
    columns = ',\n  '.join('%s %s' % x for x in column_types)
    template_create = """CREATE TABLE %(name)s (
                      %(columns)s
                    );"""    
    #print 'COLUMNS:\n', columns
    create = template_create % {'name' : name, 'columns' : columns}
    return create

def renameColumns(dataframe):
    initDict = dataframe.dtypes.to_dict()
    renames = {}
    # First make the colum names postgres compatiable, even though technically not required.
    for k,v in initDict.items():
        col = k.replace(' ','_')
        col = col.replace('Extended','Extn')
        col = col.replace('Range','Rng')
        col = col.replace('Requested','Req')
        col = col.replace('Speed','Spd')
        col = col.replace('Control','Cntrl')
        col = col.replace('Upper','Up')
        col = col.replace('Limit','Lmt')
        col = col.replace('Aftertreatment','Aftrtmnt')
        col = col.replace('After_Treatment','Aftrtmnt')
        col = col.replace('Engagement','Engmnt')
        col = col.replace('Selective_Catalytic_Reduction','SCR')
        col = col.replace('Diesel_Particulate_Filter','DPF')
        col = col.replace('Maximum','Max')
        col = col.replace('Momentary','Momnt')
        col = col.replace('Override','Over')
        col = col.replace ('Temperature','Temp')
        col = col.replace ('Diesel_Exhaust_Fluid','DEF') 
        col = col.replace ('Total','Tot')
        col = col.replace ('Number','Num')
        col = col.replace ('Average','Avg')
        col = col.replace ('Distance','Dist')
        col = col.replace ('Between','Btwn')
        col = col.replace ('System','Systm')
        col = col.replace ('Recirculation','Recir')
        col = col.replace ('Absolute','Abslt')
        col = col.replace ('Pressure', 'Presr')
        col = col.replace ('Cooler','Cool')
        col = col.replace ('Variable','Var')
        col = col.replace ('Geometry','Geom')
        col = col.replace ('Turbocharger','TrbCh')
        col = col.replace ('Shutoff','Shof')
        col = col.replace ('Regeneration','Regen')
        col = col.replace ('Passive','Pasv')
        col = col.replace ('Active','Actv')
        col = col.replace ('Preliminary','Prelim')
        col = col.replace ('Commanded','Cmnd')
        col = col.replace ('Recirculation','Recir')
        col = col.replace ('intermediate','Intmdt')
        col = col.replace ('Lower','Low')
        col = col.replace ('configuration','Config')
        col = col.replace ('Compressor','Cmprsr')
        col = col.replace ('Transmission','Trnsmsn')
        col = col.replace ('Configuration','Config')
        col = col.replace ('Cleaning','Clean')
        col = col.replace ('Seconds','sec')
        col = col.replace ('binary','bnry')
        col = col.replace ('count','cnt')        
        col = col.replace('-','_')
        col = col.replace('(','')
        col = col.replace(')','')
        col = col.replace('%','pct')
        col = col.replace("'","")
        col = col.replace('/','_')
        col = col.replace('#','')
        col = col.replace('^','') 
        col = col.replace('.','_') 
        col = col.replace(':','')   
        col = col.replace ('_','')
                    
        renames[k] = col
        
    df = dataframe.rename(columns=renames)  
    return df

##############################################################################
    def test_sqlite(name, testdf):
        #print ('\nsqlite, using detect_types=sqlite3.PARSE_DECLTYPES for datetimes')
        import sqlite3
        with sqlite3.connect('test.db', detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            #conn.row_factory = sqlite3.Row
            write_frame(testdf, name, con=conn, flavor='sqlite', if_exists='replace')
            df_sqlite = read_db('select * from '+name, con=conn)    
            print ('loaded dataframe from sqlite', len(df_sqlite) )  
            print ('done with sqlite')
    
def test_postgresql(name, testdf):
    #from pg8000 import DBAPI as pg
    import psycopg2 as pg
    print ('\nPostgresQL, Greenplum')   
    pgcn = pg.connect(YOURCONNECTION)
    print ('df frame_query')
    try:
        write_frame(testdf, name, con=pgcn, flavor='postgresql', if_exists='replace')   
        print ('pg copy_from')    
        postgresql_copy_from(testdf, name, con=pgcn)    
        df_gp = read_db('select * from '+name, con=pgcn)    
        print ('loaded dataframe from greenplum', len(df_gp))
    finally:
        pgcn.commit()
        pgcn.close()
    print ('done with greenplum')

 
def upload_mysql(table_name, dataframe):
    
    import mysql.connector
    print ('\nmysql')
    #cn= MySQLdb.connect(YOURCONNECTION)
    newhost = '127.0.0.1'
    #newhost = '127.0.0.1'
    cn = mysql.connector.connect(host=newhost,
                             database='hdv',
                             user='hperugu',
                             password='perugu05')    
    try:
        write_frame(dataframe, name=table_name, con=cn, flavor='mysql', if_exists='append')
        df_mysql = read_db('select * from '+name, con=cn)    
        print ('loaded dataframe from mysql', len(df_mysql))
    finally:
        cn.close()
    print ('mysql done')


##############################################################################

if __name__=='__main__':

    
    from pandas import DataFrame
    from datetime import datetime
    import glob
    import gc
    #print ('Aside from sqlite, you will need to install the driver and set a valid connection string for each test routine.')
    
    test_data = {
        "name": [ 'Joe', 'Bob', 'Jim', 'Suzy', 'Cathy', 'Sarah' ],
        "hire_date": [ datetime(2012,1,1), datetime(2012,2,1), datetime(2012,3,1), datetime(2012,4,1), datetime(2012,5,1), datetime(2012,6,1) ],
        "erank": [ 1,   2,   3,   4,   5,   6  ],
        "score": [ 1.1, 2.2, 3.1, 2.5, 3.6, 1.8]
    }
    #test_df = DataFrame(test_data)
    #test_name = 'test_df'
    #name='test_df'
    #test_sqlite(name, df)
    #test_oracle(name, df)
    #test_postgresql(name, df)    
    #test_mysql(test_name, test_df)   
    wdir = 'W:\\In-House Research\\2018_Antelope Valley Bus Study\\Data\\CSV_Data'
    csv_files = glob.glob(wdir+'\\Bus4359_3_28_2018.*.csv')
    
    name = 'avta_dsl_raw'
    size = 500
    for file in csv_files:
        df = pd.read_csv(file, sep=',',  skiprows=0,low_memory=False)
        busnum = file.split('\\')[-1][0:7]
        fuel = 'DSL'
        newdf = renameColumns(df)
        #pdb.set_trace()
        #newdf = df
        list_of_dfs = [newdf.loc[i:i+size-1,:] for i in range(0,len(newdf),size)]
        for small_df in list_of_dfs:
            busNumList = [busnum  for i in range(len(small_df)) ]
            busFlList = [fuel for i in range(len(small_df))]
            small_df['vehNum'] = busNumList
            small_df['fuel'] = busFlList
            #pdb.set_trace()
            upload_mysql(name,small_df)
            del small_df
            gc.collect()
    
     
    
    print ('done')
