# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 11:08:11 2018

@author: hperugu
"""
import psycopg2
import psycopg2.extras as ext
from config import config
import pandas as pd
import numpy as np
import pdb
import geopandas as gpd
from psycopg2 import sql



csv_schema = """Date,Time,Latitude, Longitude,Velocity""".split(',')
csv_schema = [x.strip() for x in csv_schema]
dtype_list = {
    'Date' :          np.str,
    'cTime':          np.str,
    'Latitude':       np.float64,
    'Longituder':     np.float64,
    'velocity' :      np.float64
 }   

def read_files(fname):
    df = pd.read_csv(fname,na_values=[""], header=0,usecols=csv_schema, dtype=dtype_list )
    
    df = df.replace({0.0:np.nan})
    # We are going to fill Nan with back as forward fill
    df = df.fillna(method='ffill')
    df = df.fillna(method='bfill')
    #df.loc[df['Latitude']==0.00]
    for fieldName in csv_schema:
        if fieldName in dtype_list:
            df[fieldName] = df[fieldName].astype(dtype_list[fieldName])
    df = df.drop(['Date'], axis=1)  
    return df

def read_routes(route_file):
    """This function reads routes stanrt&end shapefile into spatial table"""
    routes_df = gpd.read_file(route_file)
    commands = ("DROP TABLE IF EXISTS {}",
                "CREATE TABLE {} (ROUTE_NAME CHAR(50), ST_END CHAR(50),Longitude numeric, Latitude numeric)")
                
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        
        for com in commands:
            cur.execute(sql.SQL(com).format(sql.Identifier("avta_routes")))
        #insrt = "INSERT {0} (ROUTE_NAME,ST_END,Longitude,Latitude) VALUES {1}"
        insrt = "INSERT INTO {} VALUES (%s, %s, %s, %s)" 
        for i, row in routes_df.iterrows():
            cur.execute(sql.SQL(insrt).format(sql.Identifier("avta_routes")),[row['ROUTE_NAME'],'Start',row['Strt_Long'],row['Strt_Lat']])
            
        for i, row in routes_df.iterrows():
            cur.execute(sql.SQL(insrt).format(sql.Identifier("avta_routes")),[row['ROUTE_NAME'],'End',row['End_Long'],row['End_Lat']])
            # close communication with the PostgreSQL database server
        
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_tables(table_name):
    """ Create tables in the PostgreSQL database"""
    
    commands = ("""DROP TABLE IF EXISTS {}""",
               """CREATE TABLE {} (cTime TIMESTAMP WITHOUT TIME ZONE, latitude FLOAT,longitude FLOAT, velocity FLOAT
        )
        """)
    for command in commands:
        perform_action( sql.SQL(command).format(sql.Identifier(table_name)))

            
          

def insert_list(table_name, var_list):
    """ insert multiple vendors into the vendors table  """
    #sl = "INSERT INTO vendors(vendor_name) VALUES(%s)"
    conn = None
    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
     
        cur.extecute(sql.SQL("PREPARE fooplan ( TIMESTAMP, NUMERIC,NUMERIC,NUMERIC) AS INSERT INTO {} VALUES($1, $2, $3, $4)").format(sql.Identifier(table_name)))
        ext.execute_batch(cur, "EXECUTE fooplan( %s, %s,%s,%s);", var_list)
        cur.execute("DEALLOCATE fooplan")
        #cur.executemany(sql,vendor_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
          
def make_spatial(table_name) :
    pdb.set_trace()
    """ Makes tables spatially enabled"""
    cmnd1 = "DROP INDEX IF EXISTS {}"
    commands = ("ALTER TABLE {} ADD COLUMN geom geometry(Point,4326)",
        "UPDATE {} SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326)")
    idxcmnd = "CREATE INDEX {0} ON {1} USING GIST (geom)"
    
    perform_action(sql.SQL(cmnd1).format(sql.SQL('_').join([sql.SQL(table_name), sql.SQL('gix')])))
            
    
    for command in commands:
        perform_action(sql.SQL(command).format(sql.Identifier(table_name)))
    pdb.set_trace()
    perform_action(sql.SQL(idxcmnd).format(sql.SQL('_').join([sql.SQL(table_name), sql.SQL('gix')]),sql.Identifier(table_name)))
  
                      
def join_RtesPts(table_name):
    """Joining Routes Spatial table with Points Table"""
    commands = ("ALTER TABLE {} ADD COLUMN route_name CHAR(50)",
                "ALTER TABLE {} ADD COLUMN Start CHAR(50)",
                "ALTER TABLE {} ADD COLUMN End CHAR(50)",
                "UPDATE {} as p  SET route_name = r.ROUTE_NAME FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,15), p.geom)",
                "UPDATE {} as p  SET Start = 'Start' FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,15), p.geom) and r.St_End ='Start' ",
                "UPDATE {} as p  SET End = 'End' FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,15), p.geom) and r.St_End = 'End' ")
    # create table one by one
    for command in commands:
        perform_action(sql.SQL(command).format(sql.Identifier(table_name)))

            
def insrt_Rte(table_name)             :
    stTimecmd = "SELECT min(time),route_name FROM {} WHERE Start = 'Start'"
    edTimecmd = "SELECT max(time),route_name FROM {} WHERE Start = 'End'"
    updcmd = "UPDATE {} SET route_name = %s WHERE time >= %s and time =< %s"
    strow= perform_qry(sql.SQL(stTimecmd).format(sql.Identifier(table_name)))
    stTime = strow[0]; st_Rt = strow[1]
    edrow= perform_qry(sql.SQL(edTimecmd).format(sql.Identifier(table_name)))
    edTime = edrow[0]; ed_Rt = edrow[1]
    if st_Rt != ed_Rt:
        print ("The Lat/Lon of file are not falling on Route"+st_Rt)
    else:
        perform_action(sql.SQL(updcmd).format(sql.Identifier(table_name)),[st_Rt,stTime,edTime])

                
    

def geo_process(tabl1,tabl2):
    """ Create Buffer shapefiles"""
    """ Create Interesection of buffers"""
    buffcmnd = "CREATE TABLE {0} AS \
                SELECT (ST_Union(ST_Buffer(ST_MakePoint(longitude,latitude)::geography, 10)::geometry)) \
                AS union_geom FROM {1}" 
    intrtablName = tabl1+tabl2
    intercmnd = "CREATE TABLE  {0} AS \
                SELECT (ST_INTERSECTION(a.union_geom,b.union_geom)::geometry) AS \
                inter_geom FROM {1} AS a, {2} as b"
    
    for tbl in [tabl1,tabl2]:
        perform_action(sql.SQL(buffcmnd).format(sql.Identifier(tbl)))
    perform_action(sql.SQL(intercmnd).format(sql.Identifier(intrtablName)))


def slct_fnlQry(table_name):  
    """ query parts from the parts table """
    qrycmnd = sql.SQL("SELECT cTime, Velocity, ROUTE_NAME,Strt,End,TripID FROM {0},{1} WHERE ST_Intersects({0}.geom,{1}.geom) ORDER BY cTime").format(sql.Identifier(table_name))
    results = perform_qry(qrycmnd)
    return results
            
            
def insrt_TripID(rows):
    """ Read query results into Dictionary and insert TripID into it"""
    rsltDict = {}
    for row in rows:
        [cTime,Velocity, ROUTE_NAME,Strt,End,TripID] = row
        if not cTime in rsltDict:
            rsltDict[cTime]={}
            if Strt == 'Strt'and ROUTE_NAME is not NULL:
                TripID = 1
            if End =='End':
                TripID += 1
                ROUTE_NAME = np.nan
            rsltDict[cTime]=[Velocity, ROUTE_NAME,TripID]
    return rsltDict
                
 
def export_toCSV(fileDict, file_output):
    with open(file_output, 'w') as fout:
        fout.write("Time, ROUTE_NAME, Velocity,TripID")
        
        for cTime in fileDict.keys():
            line =str(cTime)+','
            for vals in fileDict[cTime].values():
                line =+ ','.join([str(x) for x in vals])
            line +'\n'
            fout.write(line)
            
def perform_qry(cmnd):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(cmnd)
        rows = cur.fetchall()
        print("The number of parts: ", cur.rowcount)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
            
    return rows

def perform_action(cmnd):
    conn = None
    try:
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        cur.execute(cmnd)
        ###Do something
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        ## Do other thing
        
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    
    # Generic method To convert a datraframe to list of tuples there are multiple  ways best one following
    rootDir = "W:\\!! Section Research & Planning\\Perugu_projects\\Data\\Test\\"
    
    #var_list = df1.to_records(index=False).tolist()
    filenames = ["Bus4359_HEM3271_April1_1.csv","Bus60701_HEM1335_April3_1.csv"]
    read_routes('AVTA_busroutes.shp')
    #make_spatial("avta_routes")
    pdb.set_trace()
    for filename in filenames:
        table_name = filename[:-4]
        fullname = rootDir+'\\'+filename
        #insert_list(table_name,var_list)
        df1 = read_files(fullname)
        var_list = list(zip(*[df1[c].values.tolist() for c in df1]))
        create_tables(table_name) 
        insert_list(table_name,var_list)
        pdb.set_trace()
        make_spatial(table_name)
        join_RtesPts(table_name)
        if filename == filenames[0]:
            tabl1 = rootDir+'\\'+filenames[0][:-3]
            tabl2 = rootDir+'\\'+filenames[1][:-3]
            geo_process(tabl1,tabl2)
        rows = slct_fnlQry(table_name)
        finalDict = insrt_TripID(rows)
        final_output = table_name+'_final.csv'
        export_toCSV(final_output)
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    