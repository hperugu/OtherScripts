# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 13:20:33 2020

@author: hperugu
"""
import pandas as pd
import pdb
import datetime
import glob
import os


#elec_cols =  ['Transmission  Gear ()', 'Powertrain RPM ()','Latitude', 'Longitude','Velocity','time','Powertrain RPM ()', 
#              'Front door status ()',	'Rear door status ()','Battery Present Total Voltage ()', 'battery_present_total_current_', 
#              'Remain electricity ()','SOC ()', 'Wheelchair Ramp Status ()','Brake Switch ()', 'Kneeling Information ()', 'NumSats', 'vel_comp','Engine_Speed_rpm_corr']

#elec_cols =  ['time','Battery Present Total Voltage ()', 'battery_present_total_current_','SOC ()', 'vel_comp']
elec_cols =  ['time','battery_present_total_voltage_', 'battery_present_total_current_','soc_', 'vel_comp','longitude','latitude']
skiprowNum = 0
rootDir="C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step2"
 #Stored in seprate folder the charging info
outRootDir = "C:\\E\\RDWork\\AVTA_Analysis\\Data_fromMarch2020\\Electric_Data\\Step3\\EnergyAnalysis\\Whole_Data"
buses = [d for d in os.listdir(rootDir) if os.path.isdir(os.path.join(rootDir,d))]
#buses = ['Bus40858']

#Read CSV file
for bus in buses:
    csv_files= glob.glob(rootDir+'\\'+bus+'\\Combined\\*.csv')
    for csv_file in csv_files:
        #csv_file ='C:\\E\\RDWork\\AVTA_Analysis\\Electric\\Step2\\Bus40815\\Bus40815_8_6_2019_16.csv'
        print ("Processing "+csv_file)
        data = pd.read_csv(csv_file, sep=',', skiprows = skiprowNum,usecols=elec_cols)
        #data.rename(columns={"Battery Present Total Voltage ()": "Battery Present Total Voltage ()", "battery_present_total_current_": "battery_present_total_current_", "SOC ()": "SOC ()"})
        
        #### Bus Number
        BusNum = csv_file.split('\\')[-2]
        filename = csv_file.split('\\')[-1]
        Rtname = '_'.join(filename.split('_')[0:2])
        TripID = filename.split('_')[2]
        #pdb.set_trace()
        BusType = BusNum[3:5]
        
        
        #date = data['time'][0].split(' ')[0]
        data['date'] = data['time'].str.split(' ', expand=True)[0]
        
        #### Out of Depot Status
        data.loc[( data['longitude']< -118.1388) &  (data['longitude']> -118.142) & (data['latitude'] < 34.652) & (data['latitude']>34.468) & (data['latitude'] != 0),'outofdepot'] = 1
        data.loc[data['outofdepot'] != 1, 'outofdepot'] = 0
        ### Date of data
        #data['time_n']=pd.to_datetime(data['time'])
        #data['Date'] = data['time_n'].dt.strftime('%m/%d%y')
        data['vel_comp_mph'] = data['vel_comp'] *0.621371
        #data['time_sec'] = data['time_n'].diff()
        data['time_hrs'] = 1/3600
        data['distance_miles'] = data['vel_comp']*1/3600*0.621371
        data['energy_kwh'] = data['battery_present_total_voltage_'] * data['battery_present_total_current_'] * 1/1000 * 1/3600
        ##### Idling time indicator
        data.loc[data['vel_comp'] < 3.0, 'idle_status'] = 1
        data.loc[data['vel_comp'] > 3.0, 'idle_status'] = 0
        #### Charging time indicator
        data.loc[data['battery_present_total_current_'] < 0.0, 'charge_status'] = 1
        data.loc[data['battery_present_total_current_'] > 0.0, 'charge_status'] = 0
        ##### Charging Type
        #if BusNum in ['Bus60701','Bus60704','Bus60710','Bus60707']:
        #    data.loc[data['battery_present_total_voltage_'] > 1000 , 'charge_type'] = 'Wave'
        #else:
        #    data.loc[data['battery_present_total_voltage_'] > 750 , 'charge_type'] = 'Wave'
        
        
        #### Different Average Speeds
        avg_depOut_spd = data[data['outofdepot']==1][['date','vel_comp_mph']].groupby('date').mean()
        avg_depOut_spd_dict = avg_depOut_spd.to_dict()
        avg_nonidle_spd =  data[data['idle_status'] != 1][['date','vel_comp_mph']].groupby('date').mean()
        avg_nonidle_spd_dict = avg_nonidle_spd.to_dict()
        avg_tot_spd = data[data['vel_comp'] != 0][['date','vel_comp_mph']].groupby('date').mean()
        avg_tot_spd_dict = avg_tot_spd.to_dict()
        ##### Average Distance
        tot_depOut_dist = data[data['outofdepot']==1][['date','distance_miles']].groupby('date').sum()
        tot_depOut_dist_dict = tot_depOut_dist.to_dict()
        tot_dist = data[['date','distance_miles']].groupby('date').sum()
        tot_dist_dict = tot_dist.to_dict()
        ####Total times
        tot_depOut_time = data[data['outofdepot']==1][['date','time_hrs']].groupby('date').sum()
        tot_depOut_time_dict = tot_depOut_time.to_dict()
        tot_time = data[['date','time_hrs']].groupby('date').sum()
        tot_time_dict = tot_time.to_dict()
        tot_nonidle_time = data[data['idle_status'] != 1][['date','time_hrs']].groupby('date').sum()
        tot_nonidle_time_dict = tot_nonidle_time.to_dict()
        
        #### Average Energy consumption = Total Energy Consumption/Total Distance
        tot_depOut_engy = data[data['outofdepot']==1][['date','energy_kwh']].groupby('date').sum()
        tot_depOut_engy_dict = tot_depOut_engy.to_dict()
        tot_engy = data[['date','energy_kwh']].groupby('date').sum()
        tot_engy_dict = tot_engy.to_dict()
        tot_nonidle_engy = data[data['idle_status'] != 1][['date','energy_kwh']].groupby('date').sum()
        tot_nonidle_engy_dict = tot_nonidle_engy.to_dict()
        tot_chrg_engy = data[data['charge_status'] != 1][['date','energy_kwh']].groupby('date').sum()
        tot_chrg_engy_dict = tot_chrg_engy.to_dict()
        
        
        #####
        
        filename = csv_file.split('\\')[-1]
        outfile_parts= filename.split('_')
        outfile = '_'.join(outfile_parts[:-1])+'_totAct.csv'
        try:
            with open(outRootDir+'\\'+outfile, 'w') as fout:
                header = 'BusNum,RtName,Date,Avg_Speed,NonIdle_Avg_Speed,DepoOut_Speed,tot_Distance,DepoOut_Distance,\
                        Tot_time,NonIdle_Tot_time,Depout_Tot_time, Tot_Engy,NonIdle_Tot_Engy,Depout_Engy,Tot_Charge_Energy\n'
                fout.write(header)
                dates = avg_depOut_spd_dict['vel_comp_mph'].keys()
                line = BusNum+','
                
                for date in dates:
                    line = BusNum +','+str(Rtname)+','+str(date)+','
                    line += str(avg_tot_spd_dict['vel_comp_mph'][date])+','
                    line += str(avg_nonidle_spd_dict['vel_comp_mph'][date])+','
                    line += str(avg_depOut_spd_dict['vel_comp_mph'][date])+','
                    line += str(tot_dist_dict['distance_miles'][date])+','
                    line += str(tot_depOut_dist_dict['distance_miles'][date]) + ','
                    line += str(tot_time_dict['time_hrs'][date]) + ','
                    line += str(tot_nonidle_time_dict['time_hrs'][date]) + ','
                    line += str(tot_depOut_time_dict['time_hrs'][date]) + ','
                    line += str(tot_engy_dict['energy_kwh'][date]) + ','
                    line += str(tot_nonidle_engy_dict['energy_kwh'][date]) + ','
                    line += str(tot_depOut_engy_dict['energy_kwh'][date]) + ','
                    try:
                        line += str(tot_chrg_engy_dict['energy_kwh'][date]) +'\n'
                    except:
                        line +='0.0\n'
                    fout.write(line)
        except:
            continue
                
           
        ######## Include Speed Bins
        data['Speed_Bin'] = round(data['vel_comp_mph']/2.5)
        #### Calculate accleration
        data['accl_mph_sec'] = data['vel_comp_mph'].diff()
        data['accl_bin'] = round(data['accl_mph_sec']/2.5)
        
       
        chargDict = {}
        idleDict = {}
        spdDict = {}
        accDict = {}
        socDict = {}
        spdActDict = {}
        accActDict = {}
        socActDict = {}
        totEng = {}
        totEngAct = {}
        
        outData = data[data["outofdepot"] == 0]
        #pdb.set_trace()
        dates = avg_depOut_spd_dict['vel_comp_mph'].keys()
        for date in dates:
            if tot_depOut_dist_dict['distance_miles'][date] < 10.0:
                    
                  print ("Too low distance to report")
                  continue

        for i, row in outData.iterrows():
            
            
            date = row['time'].split(' ')[0]
           
            soc = row['soc_']
            speed = row['vel_comp_mph']
            time = datetime.datetime.strptime(row['time'],'%m/%d/%y %H:%M:%S')
            energy = row['energy_kwh']
            distance = row['distance_miles']
            #if energy < 0.0:
            #    pdb.set_trace()
            
            if 6 < time.hour <9:
                tod ='AM'
            elif 8 < time.hour <15:
                tod = 'MID'
            elif 14 < time.hour <18:
                tod = 'PM'
            else:
                tod = 'NT'
                
            try:
                curnt = float(row['battery_present_total_current_'])
            except:
                curnt = 0.0
            
            if not date in chargDict:
                chargDict[date] ={}
                chrgCnt = 1
                chrgFreq = 1
                idleDict[date] = {}
                idleCnt = 1
                idleFreq = 1
              
                
            #pdb.set_trace()
            if soc< 100.0:
                socbin = (soc//10) +1 
            else:
                socbin = 10
            if speed > 0.0:
                
                spdbin = (speed//2.5) + 1
            else:
                spdbin = 0
            try:
                
                #accbin = (row['accl_mph_sec']//0.5) + 1
                accbin = (speed-prev_speed)//0.5 + 1
                if accbin > 10.0:
                    accbin = 10
                if accbin < -9.0:
                    accbin = -9
               
            except:
                accbin = 0

            if not date in accDict:
                accDict[date] = {}
                accActDict[date] ={}
                spdDict[date] ={}
                spdActDict[date] ={}
                socDict[date] ={}
                socActDict[date] ={}
                totEng[date] = {}
            if not TripID in totEng[date]:
                totEng[date][TripID] = {}
                totEng[date][TripID]['Energy'] = 0.0
                totEng[date][TripID]['Distance'] = 0.0 
                totEng[date][TripID]['TOD'] = tod
            ####### To take only Non Regenerative energy 
            if row['charge_status'] ==0 and energy > 0:  
                
                try:
                    spdDict[date][spdbin] += energy
                    spdActDict[date][spdbin] += distance
                except:
                    spdDict[date][spdbin] = 0.0
                    spdActDict[date][spdbin] = 0.0
                try:
                    socDict[date][socbin] += energy                
                    socActDict[date][socbin] += distance
                except:
                    socDict[date][socbin] = 0.0
                    socActDict[date][socbin] = 0.0
                
                totEng[date][TripID]['Energy'] += energy
                totEng[date][TripID]['Distance'] += distance
                
            if row['charge_status'] ==0 :
                try:
                     accDict[date][accbin] += energy
                     accActDict[date][accbin] += distance
                     
                except:
                    accDict[date][accbin] = 0.0
                    accActDict[date][accbin] = 0.0
                
                
              
                       
                
            if idleCnt == 1 :
                initIdle = time
                
                
            if speed < 3.0:
                idleCnt += 1
            #if idleCnt > 120 and speed > 2.0:
                
                
            if idleCnt > 120 and (time.hour*3600+time.minute*60+time.second) - (initIdle.hour*3600+initIdle.minute*60+initIdle.second) == idleCnt and speed > 2.0:
                #pdb.set_trace()
                endIdle = time
                idlePeriod = (endIdle.hour *60 + endIdle.minute) - (initIdle.hour*60+initIdle.minute)
                if not idleFreq in idleDict[date]:
                    idleDict[date][idleFreq] = {}
                    idleDict[date][idleFreq]['idlePeriod'] = 0.0
                idleDict[date][idleFreq]['timeofday'] = tod
                idleDict[date][idleFreq]['idlePeriod'] += idlePeriod
                idleDict[date][idleFreq]['idle_start'] =  datetime.datetime.strftime(initIdle,'%H:%M:%S')
                idleDict[date][idleFreq]['idle_end'] =  datetime.datetime.strftime(endIdle,'%H:%M:%S')
                idleFreq += 1
                initIdle = time
                
            if speed < 1.0 and curnt < 0 and row['charge_status'] ==1 :
                
                chrgCnt += 1
                
            if chrgCnt ==1:
                initsoc = soc
                stChrgtime = time
        
            #if chrgCnt > 30 and (time.hour*3600+time.minute*60+time.second) - (stChrgtime.hour*3600+stChrgtime.minute*60+stChrgtime.second) == chrgCnt and curnt > 0:
            if chrgCnt > 150 and curnt > 0 and soc > initsoc:
                #pdb.set_trace()
                chrgFreq += 1
                chrgCnt = 0
                endChrgtime = time
                endsoc = soc
                chrgPeriod = (endChrgtime.hour *60 +endChrgtime.minute) - (stChrgtime.hour *60 +stChrgtime.minute)
                if not chrgFreq in chargDict[date]:
                    chargDict[date][chrgFreq] = {}
                
                chargDict[date][chrgFreq]['Charge_time'] = chrgPeriod
                chargDict[date][chrgFreq]['Charge_st_SOC'] = initsoc
                chargDict[date][chrgFreq]['Charge_end_SOC'] = endsoc
                chargDict[date][chrgFreq]['TOD'] = tod
                #if wave == 'Wave':
                #    chargDict[date][chrgFreq]['Type'] = wave
                #else:
                #    chargDict[date][chrgFreq]['Type'] = 'wired'
                
            #try:   
            #    if (time.hour*3600+time.minute*60+time.second)- (stChrgtime.hour*3600+stChrgtime.minute*60+stChrgtime.second) > chrgCnt and curnt <0:  
            #        chrgCnt = 1
            #        initsoc = soc
            #        stChrgtime = time
                    
            #except:
            #    continue
            #try:   
            #    if (time.hour*3600+time.minute*60+time.second)- (initIdle.hour*3600+initIdle.minute*60+initIdle.second) > idleCnt and speed <3:  
            #        chrgCnt = 1
            #        initsoc = soc
            #        stChrgtime = time
                    
            #except:
            #    continue
            
            prev_speed = speed
        
        
        spdfile = '_'.join(outfile_parts)+'_spdbin.csv'
        accfile = '_'.join(outfile_parts)+'_acclbin.csv'
        idlefile = '_'.join(outfile_parts)+'_idlebin.csv'
        socfile = '_'.join(outfile_parts)+'_socbin.csv'
        tripfile = '_'.join(outfile_parts)+'_triptot.csv'
        #pdb.set_trace()
        with open(outRootDir+"\\"+spdfile, 'w') as fout:
            fout.write('Busnum,RtName,Date,spdbin,energy_Kwh,distance\n')
            for date in spdDict.keys():
            
                for spdbin in spdDict[date].keys():
                    try:
                      eng= spdDict[date][spdbin]/spdActDict[date][spdbin]
                    except:
                      if spdbin == 0.0:
                          eng =  spdDict[date][spdbin]
                    line =  str(BusNum)+','+str(Rtname)+','+str(date)+','+str(spdbin) +','+str(spdDict[date][spdbin])+','+str(spdActDict[date][spdbin])+'\n'
                    fout.write(line) 
                    
        with open(outRootDir+"\\"+socfile, 'w') as fout:
            fout.write('Busnum,RtName,Date,socbin, energy_Kwh, distance\n')
            for date in socDict.keys():
                for socbin in socDict[date].keys():
                    try:
                      eng= socDict[date][socbin]/socActDict[date][socbin]
                    except:
                      eng =  0.0
                    line = str(BusNum)+','+str(Rtname)+','+str(date)+','+ str(socbin)+','+str(socDict[date][socbin])+','+str(socActDict[date][socbin])+'\n'
                    fout.write(line)
                    
        with open(outRootDir+"\\"+tripfile, 'w') as fout:
            fout.write('Busnum,RtName,Date,TripID,energy_Kwh,distance\n')
            for date in totEng.keys():
                for tripid in totEng[date].keys():
                    line = str(BusNum)+','+str(Rtname)+','+str(date)+','+str(tripid) +','
                    for var in ['Energy','Distance','TOD']:
                        line += str(totEng[date][tripid][var])+','
                    line += '\n'
                    fout.write(line) 
                 

                    
        with open(outRootDir+"\\"+accfile, 'w') as fout:
            fout.write('Busnum,RtName,Date.acclbin, energy_Kwh,distance\n')
            for date in accDict.keys():
                for accbin in accDict[date].keys():
                    try:
                      eng= accDict[date][accbin]/accActDict[date][accbin]
                    except:
                      eng =  0.0
                    line = str(BusNum)+','+str(Rtname)+','+str(date)+','+ str(accbin)+','+str(accDict[date][accbin])+','+str(accActDict[date][accbin])+'\n'
                    fout.write(line) 
                
         
        
        with open(outRootDir+"\\"+idlefile, 'w') as fout:
            fout.write('Busnum,RtName,Date,idleNum,idlePeriod_minutes,idle_start, idle_end, timeofday\n')
            for date in idleDict.keys():
                for idleFreq in idleDict[date].keys():
                    line = str(BusNum)+','+str(Rtname)+','+str(date)+','+str(idleFreq)+','
                    for var in ['idlePeriod','idle_start','idle_end','timeofday']:
                        line += str(idleDict[date][idleFreq][var])+','
                    line += '\n'
                    #pdb.set_trace()
                    fout.write(line) 
                    
                
    #pdb.set_trace()
    ##Write Summary file as csv 