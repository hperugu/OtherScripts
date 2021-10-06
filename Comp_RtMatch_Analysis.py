# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 11:08:11 2018

@author: hperugu
"""


import psycopg2 #For copying files from one dfolder another

import psycopg2.extras as ext
from config import config #### IT is for connecting to postgres database
import pandas as pd ## for data reading
#import numpy as np ### wrangling teh data.
import pdb # for debugging
import geopandas as gpd
from psycopg2 import sql ## running sql queries within Python
from sqlalchemy import create_engine ## connecting to sql database
import io
import glob
import os
import shutil
from datetime import timedelta  
import datetime as dt
import gdal

'''Since this evoluting script both psycopg2 and sqlalchemy packages were used'''
'''This script is kind of convoluted'''
'''Step 0: Read Routes File into a spatial table'''
'''Step 1: Read the CSV data into a regular table'''
'''Step 2: Make it Spatial table'''
'''Step 3: Join the Route Ends with trajectory table created in step2'''
'''Step 4: A logical process is condcuted to remove least possible routes and trips '''
'''Step 5: Using QA ed time based trip and route numbers, update existing table'''
'''Step 6: Export the data into CSV file for analysis'''
'''Step 7: a seperate method was written to find the common route from lat/lon data'''
LocalRtdistDict= {'AV005 E':6.52926820781623, 'AV005 W':6.53594361628878, 'AV007 N':19.5030823073389, 'AV007 S':19.5030823073389,\
            'AV001 N':15.157447175537, 'AV001 S':15.4027105499403, 'AV011 E':23.8296322572196,'AV011 W':21.4228420699881,\
            'AV003 E':8.3880012227327, 'AV003 W':9.89496743156325,'AV006 E':12.6030708707041,'AV006 W':15.0256966202267,\
            'AV009 E':9.47229480590692,'AV009 W':5.8293094628401,'AV004 N':10.6913037087709,'AV004 S':11.4831444442721,\
            'AV002 E':8.13623879689738,'AV002 W':8.62387385011933,'AV0041 N':12.6374887835919,'AV0041 S':13.4293295191527,\
            'AV0052 E': 23.13, 'AV0052 W':21.76,'AV008 N': 12.1, 'AV008 S': 12.2}

CommRtdistDict = {'AV785 N':124.689407531623,'AV785 S':96.8815244767303,'AV786 N':111.579493894988,'AV786 S':87.4812543973747,\
                  'AV787 N':106.455154809069,'AV787 S':84.0234391587112,'AV7861 S':85.8503426103819,'AVLAL E':25.2085755632458,\
                  'AVLAL W':28.1994792615752,'AVLAP E':20.5448981973747,'AVLAP W':23.9446535300716,'AV747 N': 38.0,\
                  'AV747 S': 38.3, 'AV790 S':36.5,'AV790 N':36.5,'AV748 N': 42.3, 'AV748 S': 42.8 }

def read_pd2Table(csv_file,table_name,skiprowNum):
    data = pd.read_csv(csv_file, sep=',',  skiprows=skiprowNum)
    initDict = data.dtypes.to_dict()
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
    conn = None
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        data.head(0).to_sql(table_name, engine,if_exists='replace',index=False) #truncates the table
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
           
def read_routes(route_file):
    """This function reads routes stanrt&end shapefile into spatial table"""
    routes_df = gpd.read_file(route_file)
    commands = ("DROP TABLE IF EXISTS {}",
                "CREATE TABLE {} (ROUTE_NAME VARCHAR,TRIPST VARCHAR,TRIPEND VARCHAR, SERVICE VARCHAR, Longitude numeric, Latitude numeric)")
    conn = None  
    try:
        
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        
        for com in commands:
            cur.execute(sql.SQL(com).format(sql.Identifier('avta_routes')))
        #insrt = "INSERT {0} (ROUTE_NAME,ST_END,Longitude,Latitude) VALUES {1}"
        insrt = "INSERT INTO {} VALUES (%s,%s, %s, %s, %s, %s)" 
        for i, row in routes_df.iterrows():
            cur.execute(sql.SQL(insrt).format(sql.Identifier('avta_routes')),[row['ROUTE_NAME'],'Start','',row['SERVICE'],row['Strt_Long'],row['Strt_Lat']])
            
        for i, row in routes_df.iterrows():
            cur.execute(sql.SQL(insrt).format(sql.Identifier('avta_routes')),[row['ROUTE_NAME'],'','End',row['SERVICE'],row['End_Long'],row['End_Lat']])
            # close communication with the PostgreSQL database server
        
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

          
def make_spatial(table_name) :
    """ Makes tables spatially enabled"""
    """" DELETE FROM {} WHERE longitude = 0 or latitude =0","""
    commands = (
        "DELETE FROM {} WHERE longitude = 0 or latitude =0",
        "ALTER TABLE {} ADD COLUMN geom geometry(Point,4326)",
        "UPDATE {} SET geom=ST_SetSRID(ST_MakePoint(longitude,latitude),4326)")
    idxcmnd = "CREATE INDEX {0} ON {1} USING GIST (geom)"
    
    conn = None
    try:
        
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(sql.SQL(command).format(sql.Identifier(table_name)))
        cur.execute(sql.SQL(idxcmnd).format(sql.SQL('_').join([sql.SQL(table_name), sql.SQL('gix')]),sql.Identifier(table_name)))
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        #pdb.set_trace()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
                      
def join_RtesPts(table_name,buffer):
    """Joining Routes Spatial table with Points Table"""
    #pdb.set_trace()
    precmnd = "ALTER TABLE {} ADD COLUMN route_name VARCHAR,ADD COLUMN rte_strt VARCHAR, ADD COLUMN rte_end VARCHAR, ADD COLUMN service VARCHAR, ADD COLUMN tripid int "
    commands = ("UPDATE {} as p  SET route_name = r.route_name FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,%s)::geometry, p.geom)",
                "UPDATE {} as p  SET rte_strt = 'Start' FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,%s)::geometry, p.geom) and r.tripst ='Start' ",
                "UPDATE {} as p  SET rte_end = 'End' FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,%s)::geometry, p.geom) and r.tripend = 'End' ",
                "UPDATE {} as p  SET service = r.service FROM avta_routes as r WHERE ST_Contains(ST_Buffer(r.geom::geography,%s)::geometry, p.geom) ")
    conn = None
  
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL(precmnd).format(sql.Identifier(table_name)))
        
        for command in commands:
            cur.execute(sql.SQL(command).format(sql.Identifier(table_name)),[buffer])
            
        conn.commit()
        cur.close()
          
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()                             

    

def fix_RteTrp_Qry(table_name,service):  
    """ query parts from the parts table """
    """ This method is used select the records to update tripd numbers"""
    conn = None
    
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT Time, vel_comp, route_name,rte_strt,rte_end, tripid FROM {} WHERE route_name IS NOT NULL AND service = %s ORDER BY Time").format(sql.Identifier(table_name)),[service])
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()  
            
def distBasedQry (table_name,rtdistDict):
    
    conn = None
    #pdb.set_trace()
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        cur.execute(sql.SQL("SELECT time, route_name FROM {} WHERE route_name IS NOT NULL ORDER BY time asc").format(sql.Identifier(table_name)))
        #cur.execute(sql.SQL("UPDATE {}  SET rte_strt = NULL, rte_end = NULL").format(sql.Identifier(table_name)))
        rows = cur.fetchall()
        new_table = table_name+'_dist'
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} ").format(sql.Identifier(new_table)))
        cur.execute(sql.SQL("CREATE TABLE {0} AS SELECT time, SUM(vel_comp*0.000172603) OVER (ORDER BY time ASC) AS distrun FROM {1} ORDER BY time").format(sql.Identifier(new_table),sql.Identifier(table_name)))
        conn.commit()
        newrows = []
        #print ("Time before this loop:"+str(dt.datetime.now()))
        simpDict ={}
         
        for rtp in rows:
            rt = rtp[1]
            nSt_time = rtp[0]
            if not rt in simpDict:
                simpDict[rt] = []
            simpDict[rt].append(dt.datetime.strptime(nSt_time,'%m/%d/%y %H:%M:%S'))
            cStTime = simpDict[rt][-1]
            try:
                pStTime = simpDict[rt][-2]
            except:
                pStTime = simpDict[rt][-1]
                
            if ((cStTime.day*1440+cStTime.hour*60+cStTime.minute)-(pStTime.day*1440+pStTime.hour*60+pStTime.minute) < 25 ) :
                newrows.append((nSt_time,rt))
            if rt not in rtdistDict.keys():
                continue
            #### For Diesel buses
            #if rt not in exprsList:
            #    cur.execute(sql.SQL("UPDATE {}  SET route_name = NULL WHERE  route_name  = %s ").format(sql.Identifier(table_name)),[rt])
            #    conn.commit()
            #    continue    
                
            #else:             
            #    newrows.append((nSt_time,rt))
                
        
        rtList = []
        
        for rtuple in newrows:
            rt = rtuple[1]
            if rt not in rtdistDict.keys():
                continue
            ndist = rtdistDict[rt]
            nSt_time = rtuple[0]
            st_time, end_time = distEndTime(table_name,rt,ndist,nSt_time)
            
            if end_time is not None and rt not in rtList:
                rtList.append(rt)
            if end_time is None :
                end_time = st_time
                #cur.execute(sql.SQL("UPDATE {}  SET route_name = NULL WHERE  route_name  = %s ").format(sql.Identifier(table_name)),[rt])
                #conn.commit()
                #continue
            cur.execute(sql.SQL("UPDATE {}  SET rte_strt = 'Start' WHERE  time  = %s AND route_name = %s ").format(sql.Identifier(table_name)),[st_time,rt])
            cur.execute(sql.SQL("UPDATE {}  SET rte_end = 'End' WHERE  time  = %s AND route_name = %s ").format(sql.Identifier(table_name)),[end_time,rt])
            conn.commit()  
            
        #pdb.set_trace()
        cur.execute(sql.SQL("SELECT Time, vel_comp, route_name,rte_strt,rte_end, tripid FROM {} WHERE route_name IS NOT NULL ORDER BY Time").format(sql.Identifier(table_name)))
        newrowList = cur.fetchall()
        conn.commit()
        cur.close()
        
        return newrowList
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
           
def distEndTime(table_name,route,dist,nSt_time):
    
    conn = None
    
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        new_table = table_name+'_dist'
        #cur.execute(sql.SQL("SELECT MAX(time) FROM {} WHERE route_name = %s ").format(sql.Identifier(table_name)),[route])
        #rows = cur.fetchall()
        #st_time = rows[0][0]
        
        ##### For local routes following method works
        cur.execute(sql.SQL("SELECT distrun FROM {} WHERE time >= %s ").format(sql.Identifier(new_table)),[nSt_time])
        initDist = cur.fetchall()[0][0]
        ####### For commuter -- we need to trick teh program as teh start points are changing
        #initDist = 4.5
        #cur.execute(sql.SQL("SELECT MIN(time) FROM {} WHERE distrun >  %s ").format(sql.Identifier(new_table)),[initDist])
        #st_time = cur.fetchall()[0][0]
        finaldist = initDist + dist
        cur.execute(sql.SQL("SELECT MIN(time) FROM {} WHERE distrun >  %s ").format(sql.Identifier(new_table)),[finaldist])
        end_time = cur.fetchall()[0][0]

        cur.close()
        
        return nSt_time,end_time
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
    

def process_RteTripID(rowList,BusTrpLen,fulldate):
    """ Read query results into Dictionary and insert tripid into it"""
    """ This method has been further improved as some times different routes are assigned to same trajectories"""
    
    preDict = {}
    tripDict = {}
    Act_tripDict ={}
    sTime = None; eTime = None
    try:
        for row in rowList:
            [actTime,Vel, rte,Str,End,Trpn] = row
            if not rte in preDict and rte is not None:
                preDict[rte] = {}
                tripDict[rte] = [] 
            # Some times the bus may not go to predefined terminal, 
            # We built a logic to consider trip end if the bus stops for 15 min
            cTime = dt.datetime.strptime(actTime,'%m/%d/%y %H:%M:%S')
            #maxTripLen = BusTrpLen[rte]
            if  Str == 'Start': 
                try:
                   #if ((cTime.hour*60+cTime.minute)- (preDict[rte]['Start'].hour*60+preDict[rte]['Start'].minute) > 15) and ((cTime.hour*60+cTime.minute)- (preDict[rte]['End'].hour*60+preDict[rte]['End'].minute) < maxTripLen) :
                   if (cTime.day*1440+cTime.hour*60+cTime.minute)- (preDict[rte]['Start'].day*1440+preDict[rte]['Start'].hour*60+preDict[rte]['Start'].minute) > 5:
                       eTime = cTime
                       preDict[rte]['End'] = eTime
                                          
                except:
           
                    sTime = cTime
                    preDict[rte]['Start'] = sTime
                    crntRte = rte
                    
                    #eDateTime = cTime + timedelta(seconds=60*maxTripLen)
                    #preDict[rte]['End'] = eDateTime
                    
    
            else:
                try:
                
                    #if ((cTime.hour*60+cTime.minute)- (preDict[rte]['Start'].hour*60+preDict[rte]['Start'].minute) > 15) and ((cTime.hour*60+cTime.minute)- (preDict[rte]['End'].hour*60+preDict[rte]['End'].minute) < maxTripLen) :
                     if (cTime.hour*60+cTime.minute)- (preDict[rte]['Start'].hour*60+preDict[rte]['Start'].minute) > 5:
                        eTime = cTime
                        preDict[rte]['End'] = eTime
                        
                                          
                except:
                    
                    sTime = cTime
                    preDict[rte]['Start'] = sTime
                    
                    #eDateTime = cTime + timedelta(seconds=60*maxTripLen)
                    #preDict[rte]['End'] = eDateTime
                    crntRte = rte       
            for rte in tripDict.keys():
                
                if set(['Start','End']).issubset(set(preDict[rte].keys())) and preDict[rte]['Start'] is not None and preDict[rte]['End'] is not None :
                    tripDict[rte].append([preDict[rte]['Start'],preDict[rte]['End']])
                    preDict[rte]['Start'] = None ; preDict[rte]['End'] = None
      

    except:
        
        pass
    
   
   
    newTripDict = scrub_dict(tripDict)
    rtList = sorted(newTripDict.keys())
  
    
     ########## Clean trips happening at same time####################
    # altNewTripDict = {}
    #for pos1 in range(0,len(rtList)):
    #    pos2 = 0
    #    rt = rtList[pos1]
    #    for nrt in newTripDict:
    #        if rt == nrt:
    #          timest = tripDict[nrt]
    #        else:
    #          continue
    #        triplen = (timest[1].day*1440+timest[1].hour*60+timest[1].minute)-(timest[1].day*1440+timest[0].hour*60+timest[0].minute)
    #        if pos2 == 0:
    #            beg = timest[0].hour*60+timest[0].minute
                
                
             
     #      try:
     #           nextRt = rtList[pos1+1]
     #       except:
     #           nextRt = rtList[0]
            

     #       newtime = tripDict[nextRt]
            
     #       if pos2 == 0:
     #           newbeg = newtime[1].day*1440+newtime[1].hour*60+newtime[1].minute
     #           newend = newtime[0].day*1440+newtime[0].hour*60+newtime[0].minute
     #           newtriplen = newend-newbeg
                    
                
     #      if triplen > newtriplen or triplen == newtriplen:
     #           try:
     #               altNewTripDict[rt].append([newtime[0],newtime[1]])
     #           except:
     #               altNewTripDict[rt] = []
     #               altNewTripDict[rt].append([timest[0],timest[1]])
     #           pos1 += 1
     #       else:
     #           try:
     #               altNewTripDict[nextRt].append([newtime[0],newtime[1]])
     #           except:
     #               altNewTripDict[nextRt] = []
     #               altNewTripDict[nextRt].append([timest[0],timest[1]])
     #           pos1 += 2 

                    
     #       pos2 += 1
         

    #TFollowing code actually develops a dictionary to save the start and end times aof each trip
    for rt in newTripDict:
        trpn = 1; i =1
        for n in range(0,len(newTripDict[rt])):
            times = newTripDict[rt][n]   
            #if (times[1].day*1440+times[1].hour*60+times[1].minute)-(times[0].day*1440+times[0].hour*60+times[0].minute) < 25 :
            #   continue
            #else:
            
            rtf, di = rt.strip().split(' ')
            if di == 'S' : di2 = 'N'
            if di == 'N' : di2 = 'S'
            if di == 'E' : di2 = 'W'
            if di == 'W' : di2 = 'E'
            rtb = rtf +' '+ di2
            for r in [rt,rtb]:
                if not r in Act_tripDict:
                    Act_tripDict[r] = {}
                if not trpn in Act_tripDict[r]:
                    Act_tripDict[r][trpn] = {}
            
            if i%2 == 1:
                Act_tripDict[rt][trpn]['Strt'] = times[0].strftime('%m/%d/%y %H:%M:%S')       
                Act_tripDict[rt][trpn]['End'] = times[1].strftime('%m/%d/%y %H:%M:%S')
            if i%2 == 0:
                Act_tripDict[rtb][trpn]['Strt'] = times[0].strftime('%m/%d/%y %H:%M:%S')       
                Act_tripDict[rtb][trpn]['End'] = times[1].strftime('%m/%d/%y %H:%M:%S')
                trpn += 1
            i += 1
            
    #pdb.set_trace()   
    penult_Dict = scrub_dict(Act_tripDict)
    final_tripDict = adjustCommuter(penult_Dict)              
    return final_tripDict

def scrub_dict(d):
    if type(d) is dict:
        return dict((k, scrub_dict(v)) for k, v in d.items() if v and scrub_dict(v))
    else:
        return d

def adjustCommuter(Act_tripDict):
     anothTripdict = {}
     for rt in Act_tripDict.keys():
         for trip in Act_tripDict[rt]:
             rtf, di = rt.strip().split(' ')
             hr = dt.datetime.strptime(Act_tripDict[rt][trip]['Strt'],'%m/%d/%y %H:%M:%S').hour
             if rt in  ['AV747 N', 'AV747 S','AV748 N','AV748 S','AV785 N','AV785 S','AV786 N','AV786 S','AV7861 S','AV787 N','AV787 S', 'AVLAL E','AVLAL W', 'AVLAP E', 'AVLAP W','AV790 N','AV790 S']:
                if di=='S' or di=='N': 
                    if hr < 12:
                         di = 'S'
                    else:
                         di = 'N'
                if di=='E' or di=='W':
                     if hr < 12:
                         di = 'W'
                     else:
                         di = 'E'
                rtf += ' '
                newrt = rtf+di
                if not newrt in anothTripdict:
                    anothTripdict[newrt] = {}
                if not trip in anothTripdict[newrt]:
                    anothTripdict[newrt][trip] = {}
                anothTripdict[newrt][trip]['Strt'] = Act_tripDict[rt][trip]['Strt']
                anothTripdict[newrt][trip]['End'] = Act_tripDict[rt][trip]['End']
                anothTripdict[newrt][trip]['TripLen'] = (dt.datetime.strptime(Act_tripDict[rt][trip]['End'],'%m/%d/%y %H:%M:%S') - dt.datetime.strptime(Act_tripDict[rt][trip]['Strt'],'%m/%d/%y %H:%M:%S')).seconds/60
             else:
                 if not rt in anothTripdict:
                     anothTripdict[rt] = {}
                 if not trip in anothTripdict[rt]:
                     anothTripdict[rt][trip] = {}
                 #pdb.set_trace()
                 anothTripdict[rt][trip]['Strt'] = Act_tripDict[rt][trip]['Strt']
                 anothTripdict[rt][trip]['End'] = Act_tripDict[rt][trip]['End']
                 anothTripdict[rt][trip]['TripLen'] = (dt.datetime.strptime(Act_tripDict[rt][trip]['End'],'%m/%d/%y %H:%M:%S') - dt.datetime.strptime(Act_tripDict[rt][trip]['Strt'],'%m/%d/%y %H:%M:%S')).seconds/60
          
          
     return anothTripdict
 
def sortBytripLen (finaldict):
    #pdb.set_trace()
    listTrip = sorted([j['TripLen'] for k, v in finaldict.items() for i, j in v.items() ])
    sort_rtes= []
    for tpln in listTrip:
        for rt in finaldict:
   
            for tid  in finaldict[rt]:
                   
                for k,v in finaldict[rt][tid].items():

                    if k=='TripLen' and v == tpln:
                        sort_rtes.append(rt)
                    
                        
    #pdb.set_trace()                      
    return sort_rtes
    
               

def insrt_RteTrip(table_name,rsltDict):
    #
    updcmd1 = "UPDATE {} SET route_name = NULL"
    updcmd12 = "UPDATE {} SET rte_strt = NULL"
    updcmd13 = "UPDATE {} SET rte_end = NULL"
    #updcmd2 = "UPDATE {} SET route_name = %s, tripid = %s WHERE time >= %s and time < %s"
    updcmd4 = "UPDATE {} SET route_name = %s,rte_strt = 'Start' WHERE time = %s "
    updcmd5 = "UPDATE {} SET route_name = %s,rte_end = 'End' WHERE time = %s"
    conn = None
    
    exprsList = ['AV747 N', 'AV747 S','AV748 N','AV748 S','AV785 N','AV785 S','AV786 N','AV786 S','AV787 N','AV787 S','AVLAL E','AVLAL W','AVLAP E','AVLAP W', 'AV7861 S','AV790 N','AV790 S']
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL(updcmd1).format(sql.Identifier(table_name)))
        cur.execute(sql.SQL(updcmd12).format(sql.Identifier(table_name)))
        cur.execute(sql.SQL(updcmd13).format(sql.Identifier(table_name)))
        conn.commit()
        #pdb.set_trace()
        
        for rtName in rsltDict:               
            for tripId in rsltDict[rtName]:
                if not rtName in exprsList and rsltDict[rtName][tripId]['TripLen'] > 80:
                    continue
                stTime = dt.datetime.strptime(rsltDict[rtName][tripId]['Strt'],'%m/%d/%y %H:%M:%S')
                endTime = dt.datetime.strptime(rsltDict[rtName][tripId]['End'],'%m/%d/%y %H:%M:%S')
                nstTime = stTime.strftime('%#m/%#d/%y %H:%M:%S')
                nendTime = endTime.strftime('%#m/%#d/%y %H:%M:%S')
                #cur.execute(sql.SQL(updcmd2).format(sql.Identifier(table_name)),[rtName,tripId,nstTime,nendTime])  
                cur.execute(sql.SQL(updcmd4).format(sql.Identifier(table_name)),[rtName,nstTime])
                cur.execute(sql.SQL(updcmd5).format(sql.Identifier(table_name)),[rtName,nendTime])
                print (rtName +"Trip Id: "+ str(tripId)+ " is updated")
                
                conn.commit()

        conn.commit()
        cur.close()
                        
    except (Exception, psycopg2.DatabaseError) as error:
            
            print(error)
    finally:
        if conn is not None:
            conn.close()   
            

            

def slct_final_Qry(table_name):  
    
    """ query parts from the parts table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT * FROM {}  ORDER BY time").format(sql.Identifier(table_name)))
        results = cur.fetchall()
        print("The number of parts: ", cur.rowcount)
        cur.close()
        return results
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
                       
     
def export_toCSV(table_name,output):
    
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        conn = engine.raw_connection()
        cur = conn.cursor()
        Expt_Qry = sql.SQL("COPY (SELECT * FROM {0} WHERE tripid IS NOT NULL) TO STDOUT DELIMITER ',' CSV HEADER").format(sql.Identifier(table_name))
        with open(output, "w") as file:    
            cur.copy_expert(Expt_Qry,file)
     
    except(Exception, psycopg2.DatabaseError) as error:
           print(error)
           
def export_toCSV_Rte(table_name,rsltDict,outDir,Date,elevArr,origX,origY,pixelH,pixelW,BusTech,busnum):
    
    routes=rsltDict.keys()
    
    exprsList = ['AV747 N', 'AV747 S','AV748 N','AV748 S','AV785 N','AV785 S','AV786 N','AV786 S','AV787 N','AV787 S','AVLAL E','AVLAL W','AVLAP E','AVLAP W', 'AV7861 S','AV790 N','AV790 S']
    try:
        engine = create_engine('postgresql+psycopg2://postgres:perugu05@localhost/postgis_25_sample')
        conn = engine.raw_connection()
        cur = conn.cursor() 
       
        for route in routes:
            for tripid in rsltDict[route]:
                #### If we want to drop those trip which include longer stops
                #if not route in exprsList and rsltDict[route][tripid]['TripLen'] > 80:
                #    continue
                
                stTime = dt.datetime.strptime(rsltDict[route][tripid]['Strt'],'%m/%d/%y %H:%M:%S')
                endTime = dt.datetime.strptime(rsltDict[route][tripid]['End'],'%m/%d/%y %H:%M:%S')
                
                #### For commuter use manual queryexprsList
                #distEndTime(table_name,route,dist,nSt_time)
                nstTime = stTime.strftime('%#m/%#d/%y %H:%M:%S')
                nendTime = endTime.strftime('%#m/%#d/%y %H:%M:%S')
                qry = sql.SQL("SELECT * FROM {0} WHERE time > {1} AND  time < {2} ORDER BY time").format(sql.Identifier(table_name),sql.Literal(nstTime),sql.Literal(nendTime))
                Expt_df =pd.read_sql(qry.as_string(cur),engine)
                if len(Expt_df) > 0 :
                    oneLastTrip(Expt_df,outDir,route,Date,tripid, rsltDict[route][tripid],elevArr,origX,origY,pixelH,pixelW,BusTech)

     
    except(Exception, psycopg2.DatabaseError) as error:
           print(error)
           
           
           
def oneLastTrip(data,outDir,route,Date,tripNum,trpnumDict,elevArr,origX,origY,pixelH,pixelW,BusTech):
    
    
    data['time'] = pd.to_datetime(data['time'],format='%m/%d/%y %H:%M:%S')
    data['idle'] = 'no'
    data.loc[data['vel_comp']< 1.0, 'idle'] = 'yes'
    idleCnt = 0
    dataDict = {}
    rtStart = None
    
  
    for i, row in data.iterrows():
        for var,val in row.items():
            if row[var] is None:
                row[var] = 0
        
        rtime = row['time'].strftime('%m/%d/%y %H:%M:%S')
        if rtime not in dataDict:
            dataDict[rtime] = {}
        
        for col in data.columns[1:]:
            dataDict[rtime][col] = row[col]
            dataDict[rtime]['route_name'] = route
            stTime = dt.datetime.strptime(trpnumDict['Strt'],'%m/%d/%y %H:%M:%S')
            endTime = dt.datetime.strptime(trpnumDict['End'],'%m/%d/%y %H:%M:%S')
            if row['time'] > stTime and row['time'] < endTime:
               row['tripid'] =tripNum
        # To get elevation from digital elevation model   
        lat = row['latitude']
        lon = row['longitude']
        try:
            elev = LatLon2Elev (lat,lon,origX,origY,pixelH,pixelW,elevArr)
            dataDict[rtime]['elevation'] = elev
        except:
            dataDict[rtime]['elevation'] = elev
            #pdb.set_trace()
        if BusTech == 'Electric':
            
            if row['vel_comp'] == 0 and row['battery_present_total_current_'] < 0 and row['idle'] == 'yes':
                dataDict[rtime]['charging_status'] = 'yes'
            else:
                dataDict[rtime]['charging_status'] = 'no'

            
                  
        if row['idle'] == 'yes':
            try: 
                if (row['time'] - idSt).seconds > 1:
                  idleCnt += 1            
            except:
                idSt = row['time'] 
            
        if row['idle'] == 'no':
            idSt = None
            idleCnt = 0
    
        if idleCnt == 900:
            if dataDict[rtime]['rte_strt'] != 'Start':
                #pdb.set_trace()
                #dataDict[idSt.strftime('%m/%d/%y %H:%M:%S')]['rte_end'] = 'End'
                rtStart = None
                rtEnd = idSt

    
    #tripNum =1
    #rteSt =0
    
    #for ntime in dataDict.keys():
    #    spntime = pd.to_datetime(ntime, format='%m/%d/%y %H:%M:%S')
        #pdb.set_trace()
    #    try:
    #        timeDiff = (spntime-prevTime ).seconds 
    #    except:
    #        prevTime = spntime
    #    for head,val in dataDict[ntime].items():
    #        if head == 'rte_strt' and val=='Start' :
    #            rteSt = 1
    #            print ("trip start time: "+ntime)
    #            stTime = pd.to_datetime(ntime, format='%m/%d/%y %H:%M:%S')
    #        if head == 'rte_end' and val =='End' and rteSt == 1 :
    #            endTime =  pd.to_datetime(ntime, format='%m/%d/%y %H:%M:%S')
    #           rteSt = 0
                #tripNum += 1
    #            print ("trip end time: "+ntime)
                
        #if (spntime-prevTime ).seconds  > 1200:
    #    try:
    #        if (endTime-stTime) > 1800:
    #            rteSt = 1
    #            tripNum += 1
    #    except:
    #        pass
    #    if rteSt ==1:
    #        dataDict[ntime]['tripid'] = tripNum
    #    else:
    #        dataDict[ntime]['tripid'] = rteSt
    #    prevTime = spntime

    rt_trip = route.replace(" ","_")+'_'+str(tripNum)
    output = outDir+'\\'+rt_trip+'_'+Date+'.csv' 
    print ("Creating file: "+output)
    #pdb.set_trace()
    with open(output,'w') as outfile:
        for ntime, heads in  dataDict.items():
            header = 'time,'+','.join(heads)
            outfile.write(header+'\n')
            break
        for  ntime, heads in  dataDict.items():
            line = ntime
            for head in heads:
                line += ','+str(dataDict[ntime][head])
            line += '\n'
            
            outfile.write(line)
            
    #print (engstCnt,idleCnt,row['time'])
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

    conn = None
    try:
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        for tbl in [tabl1,tabl2]:
            cur.execute(sql.SQL(buffcmnd).format(sql.Identifier(tbl)))
        cur.execute(sql.SQL(intercmnd).format(sql.Identifier(intrtablName)))
        conn.commit()
        cur.close()
        ###Do something
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        ## Do other thing
        
    finally:
        if conn is not None:
            conn.close()
            
def slct_common_Qry(table_name):  
    """ query parts from the parts table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT a.time, a.velocity, a.route_name,a.TripID FROM {} as a, route_inter as b WHERE ST_Contains(a.geom,b.geom) ORDER BY a.cTime").format(sql.Identifier(table_name)))
        results = cur.fetchall()
        print("The number of parts: ", cur.rowcount)
        cur.close()
        return results
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
            
    return results    

def drop_table(table_name) :
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} ").format(sql.Identifier(table_name)))
        print ("Deleted Table " +table_name)
        conn.commit()
        cur.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    

def getElevArr(elevRaster):
    # Read DEM file into an array
    ds = gdal.Open(elevRaster)
    srcband = ds.GetRasterBand(1)
    elevArr = srcband.ReadAsArray() 
    geotrans = ds.GetGeoTransform()
    # Calculate lat and lon of the ratser using llr and size of the ratser
    geotrans = ds.GetGeoTransform()
    origX = geotrans[0]
    pixelW = geotrans[1]
    origY = geotrans[3]
    pixelH = geotrans[5]
   
    return elevArr,origX,origY,pixelH,pixelW


"""Returns global coordinates from pixel x, y coords"""

def LatLon2Elev (lat,lon,origX,origY,pixelH,pixelW,elevArr):

    i = int((lon- origX)/pixelW) - 1
    j = int((lat- origY)/pixelH) - 1
    
    elev = elevArr[i][j]
    
    return elev
           
    
if __name__ == '__main__':
    
    # Generic method To convert a datraframe to list of tuples there are multiple  ways best one following
    #var_list = df1.to_records(index=False).tolist()
    #### Defaault Directories
    rootDir="C:\\E\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step2\\"
    outRootDir = "C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step3"
    """ Read AVTA Shapefile"""
    #read_routes('W:\\In-House Research\\2018_Antelope Valley Bus Study\\Data\\GIS_Data\\AVTA_BusRT_upd_4326.shp')
    #read_routes('D:\\AVTA_Project\\GIS_Data\\AVTA_BusRT_upd_4326.shp')
    ### Convert Shapefile to Postgres Spatial table and reproject it 
    #make_spatial('avta_routes')
    # DEM file location
    elevRaster = 'C:\\E\\RDWork\\AVTA_Analysis\\GIS_Data\\USGS_NED_13_n35w118_119_new.img'
    
    
    ## Trip Length Information
    ExpBusTrpLen ={'AV747 N':200, 'AV747 S':200,'AV748 N':90,'AV748 S':90,'AV785 N':140,'AV785 S':140,\
                 'AV786 N':170,'AV786 S':170,'AV787 N':140,'AV787 S':140,'AVLAL E':60,'AVLAL W':55,
                 'AVLAP E':53,'AVLAP W':55, 'AV7861 S':170, 'AV001 N':0, 'AV001 S':0,'AV002 E':0,'AV002 W':0,\
                 'AV003 E':0,'AV003 W':0,'AV004 N':0,'AV004 S':0,'AV0041 N':0, 'AV0041 S':0,\
                 'AV005 E':0,'AV005 W':0,'AV007 N':0,'AV007 S':0,'AV006 E':0,'AV006 W':0,\
                 'AV009 E':0,'AV009 W':0,'AV011 E':0,'AV011 W':0, 'AV790 N': 75, 'AV790 S':75}
    ExpBusTrpLen ={'AVLAL E':60,'AVLAL W':55,'AVLAP E':53,'AVLAP W':55,  'AV001 N':0, 'AV001 S':0,'AV002 E':0,'AV002 W':0,\
                 'AV003 E':0,'AV003 W':0,'AV004 N':0,'AV004 S':0,'AV0041 N':0, 'AV0041 S':0,\
                 'AV005 E':0,'AV005 W':0,'AV007 N':0,'AV007 S':0,'AV006 E':0,'AV006 W':0,\
                 'AV009 E':0,'AV009 W':0,'AV011 E':0,'AV011 W':0}
  
    if 'Electric' in rootDir:
        BusTech = 'Electric'
    else:
        BusTech = 'Diesel'
    ### To go through Bus directory
    buses = [d for d in os.listdir(rootDir) if os.path.isdir(os.path.join(rootDir,d))]
    if not os.path.exists(outRootDir):
        os.mkdir(outRootDir)
    #buses = ['Bus40858','Bus40857','Bus40861','Bus40862','Bus60701']
    #buses = ['Bus40858']
    buses = ['Bus60711','Bus60707','Bus60710', 'Bus60912','Bus60709']
    for bus in buses:
        path1 = os.path.join(rootDir,bus)
        destpath1 =  os.path.join(outRootDir,bus)
        if not os.path.exists(destpath1):
           os.mkdir(destpath1)
        #Monthdir = [d for d in os.listdir(path1) if os.path.isdir(os.path.join(path1,d))]
        #for month in Monthdir:
        #    path2 = os.path.join(path1,month)
        #    destpath2 = os.path.join(destpath1,month) 
            
            #Create month directories
            #if not os.path.exists(destpath2):
            #     os.mkdir(destpath2)
            
            #Days in each month
            #days = [d for d in os.listdir(path2) if os.path.isdir(os.path.join(path2,d))]
            
            #for day in days:
            #    path3 = os.path.join(path2,day)
            #    destpath3 = os.path.join(destpath2,day)
                #Create month directories
                #if not os.path.exists(destpath3):
                #    os.mkdir(destpath3)
        skiprowNum = 0
        bus_buffer = {'Commuter' : 100, 'local': 250}
        #filenames = glob.glob(path1+'\\*.csv')
        #pdb.set_trace()
        filenames = glob.glob("C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step2\\"+bus+"\\Combined\\*.csv")
        
        try:
            for fullname in filenames:
 
                filename = fullname.split('\\')[-1]
                busnumber = filename.split('_')[0]
                #pdb.set_trace()
                if int(busnumber[3:]) > 4735 and int(busnumber[3:]) < 4766:
                    bustype = 'Commuter'
                    service = 'Express Bus'
                    
                else:
                    bustype = 'local'
                    service = 'Local Bus'
                    
                bustype = 'Commuter'
                buffer = bus_buffer[bustype]
                #This input makes to use two different alogorithms for Trip Identification                
                print ("Processing  :"+filename)  
                #pdb.set_trace()                
                parts1,mo,da,yr_ext2=  filename.split('_')
                yr,ext2 = yr_ext2.split(".")
                date = da+mo
                fulldate = dt.date(int(yr),int(mo),int(da))
                table_name = parts1.lower()+date
                fulldate = yr+'-'+mo+'-'+da
                drop_table(table_name)
                read_pd2Table(fullname,table_name,skiprowNum)
                make_spatial(table_name)
                join_RtesPts(table_name, buffer)
                elevArr,origX,origY,pixelH,pixelW = getElevArr(elevRaster)
                #elevArr,origX,origY,pixelH,pixelW = 1,1,1,1,1
                #rowList = fix_RteTrp_Qry(table_name,service)  
                if bustype == 'Commuter':                 
                    rowList = distBasedQry (table_name,CommRtdistDict)
                else:
                    rowList = distBasedQry (table_name,LocalRtdistDict)
                    #pdb.set_trace()
                
                if len(rowList) == 0 :
                    with open(outRootDir+'\\'+busnumber+'_'+fulldate+'flag.csv','w') as fout:
                        fout.write("Something wrong with data. may be not enough speed information")
                        continue
                if service == 'Express Bus':
                    rsltDict = process_RteTripID(rowList,ExpBusTrpLen,fulldate)
                else:
                    rsltDict = process_RteTripID(rowList,ExpBusTrpLen,fulldate)
  
                insrt_RteTrip(table_name,rsltDict)
                #finalDict = slct_final_Qry(table_name)
                final_output = filename.split('_')[0]+'_'+filename.split('_')[1]+'_final.csv'
                export_toCSV_Rte(table_name,rsltDict,destpath1,fulldate,elevArr,origX,origY,pixelH,pixelW,BusTech,int(busnumber[3:]))
                #export_toCSV(table_name, outDir+final_output)
                drop_table(table_name)
                #pdb.set_trace()
                
            #if filename == filenames[0]:
            #tbl1 = filenames[0][:-4]; tbl2 = filenames[1][-1]
            #geo_process(tbl1,tbl2)
        except:
            #shutil.copy2(fullname, fullname.replace(rootDir,outRootDir))
            print ("Problem with file "+filename)
            #pdb.set_trace()
            continue
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    