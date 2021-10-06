# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 16:56:31 2019

@author: hperugu
"""

import os, shutil
import pdb
import glob
# Root Input Directory

# Create directorie sbased on input
#rootDir = "D:\Electric+Buses+New+processed\Bus60705"
rootDir = "/data/dump/AVTA_data_new/Raw_HEM_Data/HEM_Data/"
buses = [d for d in os.listdir(rootDir) if os.path.isdir(os.path.join(rootDir,d))]
year = ['2018']
#months={'MAR':['Mar','C']}
months = {'MAR':['Mar','C'],'APR':['Apr','D'],'SEP':['Sept','I'],'OCT':['Oct','J'],'NOV':['Nov','K'],'MAY':['May','E'],
          'JAN':['Jan','A'],'FEB':['Feb','B'],'JUN':['Jun','F'],'JUL':['Jul','G'],'AUG':['Aug','H']}
path1 = "/data/dump/AVTA_data_new/Raw_HEM_Data/"
yr = year[0]
pdb.set_tarce()
  

for bus in buses:           
    for mo in months.keys():
        #for wk in ['wk1','wk2','wk3','wk4']:
            
            for yr in year:
                path2 = os.path.join(path1,bus)
                #path5 = "D:\Diesel+Buses+New+processed\Bus4750\2018-NOV"
                path3 = os.path.join(rootDir,yr+"-"+mo)
                try:
                    print(path3)
                    os.makedirs(path3)
                except:
                    pass
                    
                for dy in range(1,31):
                    path4 = os.path.join(path3,mo+"-"+str(dy).zfill(2))
                    try:
                        os.makedirs(path4)
        
                        
                        print(path4)
                    except:
                        pass
pdb.set_trace()
                
for bus in buses:
    for mo in months.keys():
        pdb.set_trace()
        for wk in ['wk1','wk2','wk3','wk4']:
            inDir = os.path.join(rootDir,months[mo][0]+'_'+wk)
            path3 = os.path.join(rootDir,year+"-"+mo)      
            for dy in range(1,32):
                
                path4 = os.path.join(path3,mo+"-"+str(dy).zfill(2))
                src_files = glob.glob(inDir+'\8'+months[mo][1]+str(dy).zfill(2)+'*.*')
                for src_file in src_files:
                    dst_file = src_file.replace(inDir,path4)
                    try:
                        os.rename(src_file,dst_file)
                        print (src_file+'---->'+dst_file)
                    except:
                        pass     

            





            
