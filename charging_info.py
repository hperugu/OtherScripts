# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 11:39:13 2019

@author: hperugu

"""
import os 
import glob
import datetime as dt
import csv
import pdb

import sys

if __name__ == '__main__':
    rootDir="C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step2"
    #Stored in seprate folder the charging info
    outRootDir = "C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step3\\ChargingAnalysis"
    buses = [d for d in os.listdir(rootDir) if os.path.isdir(os.path.join(rootDir,d))]
    #buses = ['Bus60705','Bus60707','Bus60709','Bus60710','Bus60912']
    #buses = ['Bus60705']
    if not os.path.exists(outRootDir):
        os.mkdir(outRootDir)
    for bus in buses:
        path1 = os.path.join(rootDir,bus)
        destpath1 =  os.path.join(outRootDir,bus)
        #if not os.path.exists(destpath1):
        #    os.mkdir(destpath1)
        Monthdir = [d for d in os.listdir(path1) if os.path.isdir(os.path.join(path1,d))]
        for month in Monthdir:
            path2 = os.path.join(path1,month)
            destpath2 = os.path.join(destpath1,month) 
            #pdb.set_trace()
            #Create month directories
            #if not os.path.exists(destpath2):
            #     os.mkdir(destpath2)
            
            #Days in each month
            days = [d for d in os.listdir(path2) if os.path.isdir(os.path.join(path2,d))]
            
            #for day in days:
            #   path3 = os.path.join(path2,day)
            #    destpath3 = os.path.join(destpath2,day)
                #Create month directories
                #if not os.path.exists(destpath3):
                 #   os.mkdir(destpath3)
                
 
            filenames = glob.glob(path1+'\\Combined\\*.csv')
                #filenames = glob.glob("E:\\QAed_data\\Electric+Buses+CSV+Step2\\Bus40822\\2019-MAR\\MAR-08\\*.csv")
                
            for fullname in filenames:
                    
                    chrgCnt = 0
                    chrgFreq = 0
                    prev_soc = 0
                    filename = fullname.split('\\')[-1]
                    busnumber = filename.split('_')[0]
                    bustype = busnumber[3:5]
                    try:
                        with open(fullname, newline='') as csvfile:
                            
                            reader = csv.DictReader(csvfile) 
                            chargDict = {}
                            cnt = 0 
                            activDict = {}
                            for row in reader:
                                #pdb.set_trace()
                                cnt += 1               
                                speed = float(row['vel_comp'])
                                dist = speed *1/3600*0.621371 
                                ophr = 1/3600
                                lat = float(row['latitude'])
                                lon = float(row['longitude'])
                                if   (34.648<lat<34.652) and (-118.142<lon<-118.1388):
                                    OutofDepot = 'no'
                                elif lon == 0 or lat ==0:
                                    OutofDepot = 'notsure'
                                else:
                                    OutofDepot = 'yes'
                                try:
                                    time = dt.datetime.strptime(row['time'],'%m/%d/%y %H:%M:%S')
                                except:
                                    time = dt.datetime.strptime(row['time'].replace('/9 ','/09 '),'%m/%d/%y %H:%M:%S')
                                    pdb.set_trace()
                                date = row['time'].split(' ')[0].strip()
                                if not date in activDict:
                                    activDict[date] = {}
                                    activDict[date]['opHour'] = 0
                                    activDict[date]['miles_travel'] = 0
                                    activDict[date]['sum_speed'] = 0
                                    activDict[date]['count'] = 0
                                    activDict[date]['rev_opHour'] = 0
                                    activDict[date]['rev_sum_speed'] = 0
                                    activDict[date]['rev_count'] =0
                                    activDict[date]['rev_miles_travel'] = 0
                                
                                activDict[date]['opHour'] += ophr
                                activDict[date]['miles_travel'] += dist
                                activDict[date]['sum_speed'] += speed
                                if not ( (34.648<lat<34.652) and (-118.142<lon<-118.1388) or lat ==0.0) :
                                    activDict[date]['rev_opHour'] += ophr
                                    activDict[date]['rev_miles_travel'] += dist
                                    activDict[date]['rev_sum_speed'] += speed
                                    activDict[date]['rev_count'] += 1
                                #if speed > 3.0:
                                #    activDict[date]['count'] += 1
                                if 'Diesel' in fullname:
                                    #pdb.set_trace()
                                    continue
                                soc = float(row['soc_'])
                                try:
                                    curnt = float(row['battery_present_total_current_'])
                                except:
                                    curnt = 0.0
                               
                            
                                if not date in chargDict:
                                    chargDict[date] ={}
                      
                                if speed < 3.0 and curnt < 0:
                                    chrgCnt += 1
                                    
                                if chrgCnt ==1:
                                    initsoc = soc
                                    stChrgTime = time
                        
                                if chrgCnt > 60 and (time.hour*3600+time.minute*60+time.second)- (stChrgTime.hour*3600+stChrgTime.minute*60+stChrgTime.second) == chrgCnt and curnt > 0:
                                    try:
                                        volt = float(row['battery_present_total_voltage_'])
                                    except:
                                        volt = 0.0
                                    #pdb.set_trace()
                                    chrgFreq += 1
                                    chrgCnt = 0
                                    endChrgTime = time
                                    endsoc = soc
                                    chrgPeriod = (endChrgTime.hour *60 +endChrgTime.minute) -(stChrgTime.hour *60 +stChrgTime.minute)
                                    if not chrgFreq in chargDict[date]:
                                        chargDict[date][chrgFreq] = {}
                                    chargDict[date][chrgFreq]['Charge_time'] = chrgPeriod
                                    chargDict[date][chrgFreq]['Charge_st_SOC'] = initsoc
                                    chargDict[date][chrgFreq]['Charge_end_SOC'] = endsoc
                                    chargDict[date][chrgFreq]['Charge_start'] = stChrgTime
                                    chargDict[date][chrgFreq]['Charge_end'] = endChrgTime
                                    #if bustype =='40' and volt > 750.0:
                                    #    chargDict[date][chrgFreq]['Charge_Type'] = 'WAVE'
                                    #elif bustype =='60' and volt > 1000.0:
                                    #    chargDict[date][chrgFreq]['Charge_Type'] = 'WAVE'
                                    if OutofDepot == 'yes':
                                        chargDict[date][chrgFreq]['Charge_Type'] = 'WAVE'
                                    elif OutofDepot == 'notsure':
                                        chargDict[date][chrgFreq]['Charge_Type'] = 'NotSure'
                                    else:
                                        chargDict[date][chrgFreq]['Charge_Type'] = 'Depot'
                                    
                                    
                                try:   
                                    if (time.hour*3600+time.minute*60+time.second)- (stChrgTime.hour*3600+stChrgTime.minute*60+stChrgTime.second) > chrgCnt and curnt <0:  
                                        chrgCnt = 1
                                        initsoc = soc
                                        stChrgTime = time
                                        
                                except:
                                    continue
    
                                prev_soc = soc
                                            
                        #pdb.set_trace()    
                        fname = fullname.split('\\')[-1]
                        outfile = outRootDir+"\\"+fname[:-4]+'_activity.csv'
                        print ("writing file" + outfile)
                        with open(outfile, 'w') as out:
                           if 'Diesel' not in fullname:
                            out.write('BusNum,Date,ChargeNum,Charge_time_min,Charge_st_SOC,Charge_end_SOC,Charge_Start, Charge_End,Charge_Type,Revenue_Operating Hr,Revenue_Miles traveled, Operating Hr,Miles traveled,Rev_Avg Speed\n')
                            for date in chargDict:
                                
                                for num in chargDict[date]:
                                    line = str(busnumber)+','+str(date)+','+str(num)+','
                                    for chrg in ['Charge_time','Charge_st_SOC','Charge_end_SOC','Charge_start','Charge_end','Charge_Type']:
                                        
                                        line += str(chargDict[date][num][chrg])+','
                                    for activ in ['rev_opHour','rev_miles_travel','opHour','miles_travel']:
                                        line += str(activDict[date][activ])+','
                                    try:
                                        line += str(activDict[date]['rev_sum_speed']/activDict[date]['rev_count'])+'\n'
                                    except:
                                        line += '0.0\n'
                                    #print(line)
                                    out.write(line)
                           if 'Diesel' in fullname:
                                #pdb.set_trace()
                                out.write('BusNum, Date,Operating Hr,Miles traveled,Avg Speed\n')
                                line =  str(busnumber)+','+str(date)+','
                                for activ in ['opHour','miles_travel']:
                                    line += str(activDict[date][activ])+','
                                try:
                                    line += str(activDict[date]['sum_speed']/activDict[date]['count'])+'\n'
                                except:
                                    line += '0.0\n'
                                #print(line)
                                out.write(line)
                        
                    except:
                        print("Oops!", sys.exc_info()[0], "occured.")
                        pdb.set_trace()
                        continue


                        
                                
                            
                            