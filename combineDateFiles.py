# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 14:52:15 2020

@author: hperugu
"""
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from config import config
import pandas as pd
import io
import glob
import pdb
import os

def read_pd2Table(csv_file,table_name,skiprowNum):
    data = pd.read_csv(csv_file, sep=',',  skiprows=skiprowNum)
    initDict = data.dtypes.to_dict()
    #pdb.set_trace()
    renames = {}
    
    # First make the colum names postgres compatiable, even though technically not required.
    for k,v in initDict.items():
        col = k.replace(' ','_')
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
        col = col.lower()
        renames[k] = col
    
    data= data.rename(columns=renames)  
    #### To convert integer type of columns to float
    for v in renames.values():
        if v != 'time':
            data[v] =  pd.to_numeric(data[v], downcast='float')
   
    
    conn = None
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        data.head(0).to_sql(table_name, engine,if_exists='append',index=False) #truncates the table
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        data.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output,table_name, null="") # null values become ''
        conn.commit()
        cur.close()
    
    
       ###Catch Excption
    except (Exception, psycopg2.DatabaseError) as error:
           print(error)
            ## Do other thing
            
    finally:
       if conn is not None:
           conn.close()
          
def drop_table(tablename):
    conn = None
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        #truncates the table
        dropTableStmt   = "DROP TABLE %s;"%tablename
        # Execute the drop table command
        
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(dropTableStmt);
        conn.commit()
        cur.close()
    
    
       ###Catch Excption
    except (Exception, psycopg2.DatabaseError) as error:
           print(error)
            ## Do other thing
            
    finally:
       if conn is not None:
           conn.close()
          
           
           
def export_toCSV(table_name,output):
    
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        conn = engine.raw_connection()
        cur = conn.cursor()
        Expt_Qry = sql.SQL("COPY (SELECT * FROM {0} ORDER BY time) TO STDOUT DELIMITER ',' CSV HEADER").format(sql.Identifier(table_name))
        with open(output, "w") as file:    
            cur.copy_expert(Expt_Qry,file)
     
    except(Exception, psycopg2.DatabaseError) as error:
           print(error)
           
if __name__ == '__main__':
    
    # Generic method To convert a datraframe to list of tuples there are multiple  ways best one following
    #var_list = df1.to_records(index=False).tolist()
    rootDir="C:\\E\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step2"
    
    buses = [d for d in os.listdir(rootDir) if os.path.isdir(os.path.join(rootDir,d))]
    #buses = ['Bus60704','Bus60705','Bus60707', 'Bus60709', 'Bus60710', 'Bus60711','Bus60912']
    buses = ['Bus60912']
    skiprowNum = 0
    for bus in buses:
        #pdb.set_trace()
        filenames = glob.glob(rootDir+"\\"+bus+"\\*.csv")
        dateChk = []
        destpath1 = os.path.join( os.path.join(rootDir,bus),"Combined")
        if not os.path.exists(destpath1):
           os.mkdir(destpath1)
        for fullname in filenames:
            filename = fullname.split('\\')[-1]
            
            
            #This input makes to use two different alogorithms for Trip Identification                
            print ("Processing  :"+filename)  
            parts1,mo,da,yr, num_ext2 =  filename.split('_')
            date = da+mo
            table_name = parts1+date
            fulldate = mo+'_'+da+'_'+yr
            
            table_name = table_name.lower()
            if fulldate not in dateChk :
                drop_table(table_name)
                dateChk.append(fulldate)
                datefileslist = glob.glob(rootDir+"\\"+parts1+"\\"+parts1+"_"+fulldate+"*.csv")
                output = destpath1+"\\"+parts1+"_"+fulldate+".csv"
                #pdb.set_trace()
                for datefile in datefileslist:
                    
                    read_pd2Table(datefile,table_name,skiprowNum)
                    
                export_toCSV(table_name,output)