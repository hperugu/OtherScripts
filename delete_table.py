# -*- coding: utf-8 -*-
"""
Created on Fri May 24 09:10:10 2019

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
from sqlalchemy import create_engine

table_list = ['bus408582510',
'bus607042810',
'bus60704611',
'bus60704711',
'bus607042910',
'bus60704811',
'bus607043010',
'bus60704911',
'bus607043110',
'bus607052610',
'bus607041011',
'bus607052810',
'bus607041111',
'bus607052910',
'bus607041211',
'bus607053110',
'bus607041311',
'bus607051111',
'bus607041411',
'bus607041511',
'bus607051211',
'bus607041611',
'bus607051411',
'bus607041711',
'bus607051611',
'bus607041811',
'bus607051911',
'bus607041911',
'bus60705111',
'bus607052111',
'bus60704111',
'bus607052211',
'bus607042011',
'bus60705311',
'bus609121211',
'bus607042111',
'bus60705411',
'bus609121511',
'bus607042211',
'bus60705611',
'bus609121811',
'bus607042311',
'bus60705811',
'bus609122311',
'bus607042411',
'bus607053112',
'bus609122411',
'bus607042511',
'bus607072510',
'bus60912711',
'bus607042611',
'bus607072610',
'bus609121612',
'bus408572510',
'bus60704211',
'bus607072710',
'bus609122612',
'bus60704311',
'bus607072810',
'bus609122712',
'bus408582610',
'bus60704411',
'bus607072910',
'bus60912212',
'bus40858111',
'bus60704511',
'bus607073010',
'bus609123012',
'bus40858211',
'bus40858311',
'bus607073110',
'bus60912312',
'bus40858411',
'bus607071011',
'bus60912412',
'bus40858511',
'bus607071111',
'bus60912512',
'bus40858611',
'bus607071211',
'bus60912712',
'bus40858711',
'bus607071311',
'bus60912812',
'bus40858811',
'bus607071411',
'bus60912131',
'bus607071711',
'bus60912221',
'bus408612710',
'bus607071811',
'bus408612810',
'bus60707111',
'bus408612910',
'bus607072011',
'bus408613010',
'bus607072111',
'bus408613110',
'bus607072211',
'bus408611011',
'bus607072311',
'bus408611111',
'bus607072411',
'bus408611211',
'bus607072611',
'bus408611311',
'bus60707311',
'bus408611411',
'bus60707411',
'bus408612610',
'bus408611511',
'bus60707511',
'bus408611611',
'bus60707611',
'bus408611711',
'bus60707711',
'bus408611811',
'bus60707811',
'bus408611911',
'bus60709111',
'bus40861111',
'bus60709211',
'bus408612011',
'bus60709311',
'bus408612111',
'bus60709411',
'bus408612211',
'bus60709511',
'bus408612311',
'bus60709611',
'bus408612411',
'bus607102410',
'bus408612511',
'bus607102510',
'bus408612611',
'bus607102610',
'bus40861211',
'bus607102710',
'bus40861311',
'bus607102810',
'bus40861411',
'bus607103010',
'bus40861511',
'bus607103110',
'bus40861611',
'bus607101011',
'bus40861711',
'bus607101111',
'bus40861811',
'bus607101211',
'bus40861911',
'bus607101311',
'bus408622510',
'bus607101411',
'bus408622610',
'bus607101511',
'bus408622710',
'bus60710111',
'bus408622810',
'bus60710211',
'bus408622910',
'bus60710311',
'bus408623010',
'bus60710411',
'bus408623110',
'bus60710511',
'bus408621011',
'bus60710611',
'bus408621111',
'bus60710711',
'bus408621211',
'bus60710811',
'bus408621311',
'bus408621411',
'bus408621511',
'bus408621611',
'bus408621711',
'bus408621811',
'bus408621911',
'bus40862111',
'bus408622011',
'bus408622111',
'bus408622211',
'bus408622311',
'bus40862211',
'bus40862311',
'bus40862411',
'bus40862511',
'bus40862711',
'bus40862811',
'bus40862911']

def drop_table(table_list):
    """This function reads routes stanrt&end shapefile into spatial table"""
    conn = None  
    try:
        
        # read the connection parameters
        params = config()
        #pdb.set_trace()
        # connect to the PostgreSQL server

        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for tabl in table_list:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(tabl)))
            print (tabl+" deleted")
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
if __name__ == '__main__':
    drop_table(table_list)
    