# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 12:25:09 2019

@author: hperugu
"""

import json
import urllib.request
import pdb
pdb.set_trace()

def elevation(lat, lng):
    apikey = "AIzaSyDflRR8lkZkMPhZl-oI08qyZdWnczvMOxA"
    url = "https://maps.googleapis.com/maps/api/elevation/json"
    request = urllib.request.urlopen(url+"?locations="+str(lat)+","+str(lng)+"&key="+apikey)
    try:
        results = json.load(request).get('results')
        if 0 < len(results):
            elevation = results[0].get('elevation')
            # ELEVATION
            return elevation
        else:
            print ('HTTP GET Request failed.')
    except ValueError:
        print ('JSON decode failed: '+str(request))
    
        
elev = elevation(38.571520, -121.503367)
pdb.set_trace()
