# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:07:02 2019

@author: hperugu
"""

import utm
import glob
import pdb

#filelist = glob.glob("W:\In-House Research\2018_Antelope Valley Bus Study\Data\HEM Data\Bus4758\Process_CSV\Step2\AV785 N_2018-04-02.csv")
file = r'W:\In-House Research\2018_Antelope Valley Bus Study\Data\HEM Data\Bus4758\Process_CSV\Step2\AV785 N_2018-04-02.csv'

### For Bearing
#Formula:	θ = atan2( sin Δλ ⋅ cos φ2 , cos φ1 ⋅ sin φ2 − sin φ1 ⋅ cos φ2 ⋅ cos Δλ )
#where	φ1,λ1 is the start point, φ2,λ2 the end point (Δλ is the difference in longitude)
#Excel: (all angles in radians)
#Bearing =ATAN2(COS(lat1)*SIN(lat2)-SIN(lat1)*COS(lat2)*COS(lon2-lon1), SIN(lon2-lon1)*COS(lat2)) 
#*note that Excel reverses the arguments to ATAN2 – see notes below

### For Distance
#Law of cosines:	d = acos( sin φ1 ⋅ sin φ2 + cos φ1 ⋅ cos φ2 ⋅ cos Δλ ) ⋅ R
#Excel:	=ACOS( SIN(lat1)*SIN(lat2) + COS(lat1)*COS(lat2)*COS(lon2-lon1) ) * 6371000


with open(file,'r') as f:
        header = f.readline().strip()+',xcoord, ycoord\n'
        file_lines = header
        
        for line in f.readlines()[1:]:
            lat, lon = line.split(',')[14:16]
            
            xcoord, ycoord,zone, dir = utm.from_latlon(float(lat),float(lon),force_zone_number = 11)
            string_to_add = ','+str(xcoord)+','+str(ycoord)
            file_lines += ''.join([line.strip(), string_to_add, '\n'])
            #pdb.set_trace()
ofile = file[:-4]+'_updt.csv'
with open(ofile, 'w') as f:
        f.writelines(file_lines) 