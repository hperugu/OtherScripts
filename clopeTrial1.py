# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 15:37:12 2019

@author: hperugu
"""
import gdal
import pdb
# DEM file location

elevRaster = r'C:\Users\hperugu\Desktop\USGS_NED_13_n33w092_IMG\USGS_NED_13_n33w092_IMG.img'

# Read DEM file into an array
ds = gdal.Open(elevRaster)
srcband = ds.GetRasterBand(1)
elevArr = srcband.ReadAsArray()


# Get Lowest left Corner 

# Calculate lat and lon of the ratser using llr and size of the ratser

geotrans = ds.GetGeoTransform()
origX = geotrans[0]
pixelW = geotrans[1]
origY = geotrans[3]
pixelH = geotrans[5]



"""Returns global coordinates from pixel x, y coords"""

def LatLon2Elev (lat, lon, elevArr):

    i = int((lat- origX)/pixelW) - 1
    j = int((lon- origY)/pixelH) - 1
    
    elev = elevArr[i][j]
    
    return elev


if __name__ == "__main__":
    initlat = -91.9
    initlon = 33.5
    
    reqelev = LatLon2Elev(initlat,initlon,elevArr)
    print(reqelev)
    
# Read lat/lon into numpy array-- Ratser

# Read lat/lon from the file

# Find closest lat/lon value from  Ratser array and it's index

# Use index to extarct elevation value fromDEM raster


  

