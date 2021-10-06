# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 13:20:33 2020

@author: hperugu
"""
import pandas as pd
import pdb
import datetime
import glob

#elec_cols =  ['Transmission  Gear ()', 'Powertrain RPM ()','Latitude', 'Longitude','Velocity','time','Powertrain RPM ()', 
#              'Front door status ()',	'Rear door status ()','battery_present_total_voltage_', 'battery_present_total_current_', 
#              'Remain electricity ()','soc_', 'Wheelchair Ramp Status ()','Brake Switch ()', 'Kneeling Information ()', 'NumSats', 'vel_comp','Engine_Speed_rpm_corr']

elec_cols =  ['time','battery_present_total_voltage_', 'battery_present_total_current_','soc_', 'vel_comp']
#elec_cols =  ['time','route_name','battery_present_total_voltage_','battery_present_total_current_','soc_', 'vel_comp']
skiprowNum = 0

#Read CSV file
csv_files= glob.glob('C:\\E\\RDWork\\AVTA_Analysis\\Electric\\Step3\\Bus60710\\*.csv')
for csv_file in csv_files:
    #csv_file ='C:\\E\\RDWork\\AVTA_Analysis\\Electric\\Step2\\Bus40815\\Bus40815_8_6_2019_16.csv'
    print ("Processing "+csv_file)
    data = pd.read_csv(csv_file, sep=',', skiprows = skiprowNum,usecols=elec_cols)
    #data.rename(columns={"battery_present_total_voltage_": "battery_present_total_voltage_", "battery_present_total_current_": "battery_present_total_current_", "soc_": "soc_"})
    
    #### Bus Number
    BusNum = csv_file.split('\\')[-2]
    
    #date = data['time'][0].split(' ')[0]
    data['date'] = data['time'].str.split(' ', expand=True)[0]
    
    ### Date of data
    #data['time_n']=pd.to_datetime(data['time'])
    #data['Date'] = data['time_n'].dt.strftime('%m/%d%y')
    data['vel_comp_mph'] = data['vel_comp'] *0.621371
    #data['time_sec'] = data['time_n'].diff()
    data['time_hrs'] = 1/3600
    data['distance_miles'] = data['vel_comp']*1/3600*0.621371
    data['energy_kwh'] = data['battery_present_total_voltage_'] * data['battery_present_total_current_'] * 1/1000 * 1/3600
    ##### Idling time indicator
    data.loc[data['vel_comp'] < 5.0, 'idle_status'] = 1
    #### Charging time indicator
    data.loc[data['battery_present_total_current_'] < 0.0, 'charge_status'] = 1
    ##### Charging Type
    data.loc[data['battery_present_total_voltage_'] > 750 , 'charge_type'] = 'Wave'
    ####### Find if the vehicle left the Depot boundary
    data.loc[data['vel_comp'] > 8.0, 'outofdepot'] = 1
    
    #### Different Average Speeds
    avg_depOut_spd = data[data['outofdepot']==1][['route_name','vel_comp_mph']].groupby('route_name').mean()
    avg_depOut_spd_dict = avg_depOut_spd.to_dict()
    avg_nonidle_spd =  data[data['idle_status'] != 1][['route_name','vel_comp_mph']].groupby('route_name').mean()
    avg_nonidle_spd_dict = avg_nonidle_spd.to_dict()
    avg_tot_spd = data[data['vel_comp'] != 0][['route_name','vel_comp_mph']].groupby('route_name').mean()
    avg_tot_spd_dict = avg_tot_spd.to_dict()
    ##### Average Distance
    tot_depOut_dist = data[data['outofdepot']==1][['route_name','distance_miles']].groupby('route_name').sum()
    tot_depOut_dist_dict = tot_depOut_dist.to_dict()
    tot_dist = data[['route_name','distance_miles']].groupby('route_name').sum()
    tot_dist_dict = tot_dist.to_dict()
    ####Total times
    tot_depOut_time = data[data['outofdepot']==1][['route_name','time_hrs']].groupby('route_name').sum()
    tot_depOut_time_dict = tot_depOut_time.to_dict()
    tot_time = data[['route_name','time_hrs']].groupby('route_name').sum()
    tot_time_dict = tot_time.to_dict()
    tot_nonidle_time = data[data['idle_status'] != 1][['route_name','time_hrs']].groupby('route_name').sum()
    tot_nonidle_time_dict = tot_nonidle_time.to_dict()
    
    #### Average Energy consumption = Total Energy Consumption/Total Distance
    tot_depOut_engy = data[data['outofdepot']==1][['route_name','energy_kwh']].groupby('route_name').sum()
    tot_depOut_engy_dict = tot_depOut_engy.to_dict()
    tot_engy = data[['route_name','energy_kwh']].groupby('route_name').sum()
    tot_engy_dict = tot_engy.to_dict()
    tot_nonidle_engy = data[data['idle_status'] != 1][['route_name','energy_kwh']].groupby('route_name').sum()
    tot_nonidle_engy_dict = tot_nonidle_engy.to_dict()
    tot_chrg_engy = data[data['charge_status'] != 1][['route_name','energy_kwh']].groupby('route_name').sum()
    tot_chrg_engy_dict = tot_chrg_engy.to_dict()
    
    #####
    outdir = 'C:\\E\\RDWork\\AVTA_Analysis\\Electric\\Analysis_Folder\\'
    filename = csv_file.split('\\')[-1]
    outfile_parts= filename.split('_')
    outfile = '_'.join(outfile_parts[:-1])+'_totAct.csv'
    with open(outdir+outfile, 'w') as fout:
        header = 'BusNum,Date,Avg_Speed,NonIdle_Avg_Speed,DepoOut_Speed,tot_Distance,DepoOut_Distance,\
                Tot_time,NonIdle_Tot_time,Depout_Tot_time,Tot_Engy,NonIdle_Tot_Engy,Depout_Engy,Tot_Charge_Energy\n'
        fout.write(header)
        routes = avg_depOut_spd_dict['vel_comp_mph'].keys()
        line = BusNum+','
        
        for rt in routes:
            line = BusNum +','+str(rt)+','
            line += str(avg_tot_spd_dict['vel_comp_mph'][rt])+','
            line += str(avg_nonidle_spd_dict['vel_comp_mph'][rt])+','
            line += str(avg_depOut_spd_dict['vel_comp_mph'][rt])+','
            line += str(tot_dist_dict['distance_miles'][rt])+','
            line += str(tot_depOut_dist_dict['distance_miles'][rt]) + ','
            line += str(tot_time_dict['time_hrs'][rt]) + ','
            line += str(tot_nonidle_time_dict['time_hrs'][rt]) + ','
            line += str(tot_depOut_time_dict['time_hrs'][rt]) + ','
            line += str(tot_engy_dict['energy_kwh'][rt]) + ','
            line += str(tot_nonidle_engy_dict['energy_kwh'][rt]) + ','
            line += str(tot_depOut_engy_dict['energy_kwh'][rt]) + ','
            try:
                line += str(tot_chrg_engy_dict['energy_kwh'][rt]) +'\n'
            except:
                line +='0.0\n'
            fout.write(line)
            
    #pdb.set_trace()  
    ######## Include Speed Bins
    data['Speed_Bin'] = round(data['vel_comp_mph']/2.5)
    #### Calculate accleration
    data['accl_mph_sec'] = data['vel_comp_mph'].diff()
    data['accl_bin'] = round(data['accl_mph_sec']/0.2)
    
    
    chargDict = {}
    idleDict = {}
    spdDict = {}
    accDict = {}
    
    
    for i, row in data.iterrows():
        date = row['time'].split(' ')[0]
        rt = row['route_name']
        soc = row['soc_']
        speed = row['vel_comp_mph']
        time = datetime.datetime.strptime(row['time'],'%m/%d/%y %H:%M:%S')
        wave = row['charge_type']
        energy = row['energy_kwh']
        
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
        
        if not rt in chargDict:
            chargDict[date] ={}
            chrgCnt = 1
            chrgFreq = 1
            idleDict[date] = {}
            idleCnt = 1
            idleFreq = 1
            spdDict[rt] = {}
            accDict[rt] = {}
            
        #pdb.set_trace()
        try:
            spdbin = (speed//2.5) + 1
        except:
            spdbin =1
        try:
            
            accbin = (row['accl_mph_sec']//0.5) + 1
        except:
            accbin = 1
        
        if not spdbin in spdDict[rt]:
            spdDict[rt][spdbin] = 0.0
        if curnt > 0.0:
            spdDict[rt][spdbin] += energy
        
        if not accbin in accDict[rt]:
            accDict[rt][accbin] = 0.0
            
        accDict[rt][accbin] += energy   
        
        if idleCnt == 1:
            initIdle = time
            
        if speed < 1.5:
            idleCnt += 1
            
        if idleCnt > 30 and (time.hour*3600+time.minute*60+time.second) - (initIdle.hour*3600+initIdle.minute*60+initIdle.second) == idleCnt and speed > 3.0:
            
            endIdle = time
            idlePeriod = (endIdle.hour *60 + endIdle.minute) - (initIdle.hour*60+initIdle.minute)
            if not idleFreq in idleDict[rt]:
                idleDict[rt][idleFreq] = {}
                idleDict[rt][idleFreq]['idlePeriod'] = 0.0
            idleDict[rt][idleFreq]['timeofday'] = tod
            idleDict[rt][idleFreq]['idlePeriod'] += idlePeriod
            idleFreq += 1
            
        if speed < 1.6 and curnt < 0:
            
            chrgCnt += 1
            
        if chrgCnt ==1:
            initsoc = soc
            stChrgtime = time
    
        #if chrgCnt > 30 and (time.hour*3600+time.minute*60+time.second) - (stChrgtime.hour*3600+stChrgtime.minute*60+stChrgtime.second) == chrgCnt and curnt > 0:
        if chrgCnt > 120 and curnt > 0 and soc > initsoc:
            #pdb.set_trace()
            chrgFreq += 1
            chrgCnt = 0
            endChrgtime = time
            endsoc = soc
            chrgPeriod = (endChrgtime.hour *60 +endChrgtime.minute) - (stChrgtime.hour *60 +stChrgtime.minute)
            if not chrgFreq in chargDict[date]:
                chargDict[rt][chrgFreq] = {}
            
            chargDict[rt][chrgFreq]['Charge_time'] = chrgPeriod
            chargDict[rt][chrgFreq]['Charge_st_SOC'] = initsoc
            chargDict[rt][chrgFreq]['Charge_end_SOC'] = endsoc
            chargDict[rt][chrgFreq]['TOD'] = tod
            if wave == 'Wave':
                chargDict[rt][chrgFreq]['Type'] = wave
            else:
                chargDict[rt][chrgFreq]['Type'] = 'wired'
            
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
        
        prev_soc = soc
    
    chargfile = '_'.join(outfile_parts[:-1])+'_totChrg.csv'
    spdfile = '_'.join(outfile_parts[:-1])+'_spdbin.csv'
    accfile = '_'.join(outfile_parts[:-1])+'_acclbin.csv'
    idlefile = '_'.join(outfile_parts[:-1])+'_idlebin.csv'
    
    with open(outdir+chargfile, 'w') as fout:
        fout.write('Busnum,Date,Charge_Freq,Charge_time,Charge_st_SOC,Charge_end_SOC,Type,TOD\n')
        
        for date in chargDict.keys():
            line = ''
            for chrgFreq in chargDict[date].keys():
                line += str(BusNum)+','+str(date)+','+str(chrgFreq)+','
                for var in ['Charge_time','Charge_st_SOC','Charge_end_SOC','Type']:
                    line +=str(chargDict[date][chrgFreq][var])+','
                line += str(chargDict[date][chrgFreq]['TOD'])+'\n'
        
                fout.write(line) 
                
    with open(outdir+spdfile, 'w') as fout:
        fout.write('Busnum,Date,spdbin, energy_Kwh\n')
        
        for date in spdDict.keys():
            for spdbin in spdDict[date].keys():
                line =  str(BusNum)+','+str(date)+','+str(spdbin) +','+str(spdDict[date][spdbin])+'\n'
                fout.write(line) 
                
    with open(outdir+accfile, 'w') as fout:
        fout.write('Busnum,Date,acclbin, energy_Kwh\n')
        for date in accDict.keys():
            for accbin in accDict[date].keys():
                line += str(BusNum)+','+str(date)+','+ str(accbin)+','+str(accDict[date][accbin])+'\n'
                fout.write(line) 
       
    with open(outdir+idlefile, 'w') as fout:
        fout.write('Busnum,Date,idleNum, idlePeriod_minutes\n')
        for date in idleDict.keys():
            line = str(BusNum)+','+str(date)+','
            for idleFreq in idleDict[date].keys():
                line += str(idleFreq)+','
                for var in ['idlePeriod','timeofday']:
                    line += str(idleDict[date][idleFreq][var])+','
                line += '\n'
                fout.write(line) 
                
            
    #pdb.set_trace()
##Write Summary file as csv 