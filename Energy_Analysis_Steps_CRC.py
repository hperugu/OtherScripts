# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 15:52:33 2019

@author: hperugu
"""

""" Step 1: Check the comprehensive file has all days of Data"""
""" Step 2: Upload the data into one single table and export into day-by-day files in Raw Files Folder"""
""" Step 3: Read of each of the file and impute Velocities"""
""" Step 4: Compute Route Matching and Trip ID assignment and finally exporting into Processed Folder"""
""" Step 4a: Seperate the files based on trips into new folder"""
""" Step 5: Apply Trans Beau Modeling """
""" Step 5a: Calculate Forward Simulation and Backward Simulation for Diesel 2 Buses for  ---Validation of Comprehensive Model by Trip """
""" Step 5b:Check Fuel consumption and related energy calculation for 2 Diesel buses by trip  --- 2nd step of validation """
"""        :Thermal losses and other losses from forward simulation for each trip"""
""" Step 5c: Seperate data by Time of Day  and weekdaya vs weekend"""
""" Step 5d: Calculate trip length in miles, average trip energy demand  for each time period of the day in kw/mi and 
            seperate for weekdays  & weekends"""    
""" Step 5e: Repeat Step 5d for Electrivc buses using bacward modeling """
""" Step 5b is done to estimate auxiliary power demand - thermal efficiency """