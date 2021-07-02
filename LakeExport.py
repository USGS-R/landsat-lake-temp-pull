#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 12:07:02 2018

@author: simontopp
"""
#%%
import time
import ee
import os
#import pandas as pd
#import feather
import GEE_pull_functions as f
ee.Initialize()

#Source necessary functions.
exec(open('GEE_pull_functions.py').read())

#water = ee.Image("JRC/GSW1_1/GlobalSurfaceWater").select('occurrence').gt(80)

## Bring in EE Assets
# Deepest point for CONUS Hydrolakes from Xiao Yang
# Code available https://zenodo.org/record/4136755#.X5d54pNKgUE
dp = (ee.FeatureCollection('users/sntopp/USGS/PGDL_lakes_deepest_point')
  .filterMetadata('distance', "greater_than", 60))

#CONUS Boundary
us = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")\
    .filterMetadata('country_na', 'equals', 'United States')

## WRS Tiles in descending (daytime) mode for CONUS
wrs = ee.FeatureCollection('users/sntopp/wrs2_asc_desc')\
    .filterBounds(us)\
    .filterMetadata('MODE', 'equals', 'D')
    
pr = wrs.aggregate_array('PR').getInfo()

l8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
l7 = ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
era5 = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

#Standardize band names between the various collections and aggregate 
#them into one image collection
bn8 = ['ST_B10', 'ST_QA', 'QA_PIXEL']
bn57 = ['ST_B6', 'ST_QA', 'QA_PIXEL']
bns = ['temp','temp_qa','pixel_qa']
  
ls7 = l7.select(bn57, bns)
ls8 = l8.select(bn8, bns)

ls = ee.ImageCollection(ls7.merge(ls8))\
    .filter(ee.Filter.lt('CLOUD_COVER', 50))\
    .filterBounds(us)  


## Set up a counter and a list to keep track of what's been done already
counter = 0
done = []    

#%%

## In case something goofed, you should be able to just 
## re-run this chunk with the following line filtering out 
## what's already run. 
pr = [i for i in pr if i not in done]

for tiles in pr[0:3]:
    tile = wrs.filterMetadata('PR', 'equals', tiles)
    # For some reason we need to cast this to a list and back to a
    # feature collection
    lakes = dp.filterBounds(tile.geometry())\
        .map(dpBuff)
        
    #lakes = ee.FeatureCollection(lakes.toList(10000))
    stack = ls.filterBounds(tile.geometry().centroid())
    out = stack.map(RefPull).flatten().filterMetadata('cScore_clouds','less_than',.5)
    dataOut = ee.batch.Export.table.toDrive(collection = out,\
                                            description = str(tiles),\
                                            folder = 'EE_TempPull',\
                                            fileFormat = 'csv')#,\
                                            #selectors = [])
    #Check how many existing tasks are running and take a break if it's >15  
    maximum_no_of_tasks(25, 120)
    #Send next task.
    dataOut.start()
    counter = counter + 1
    done.append(tiles)
    print('done_' + str(counter) + '_' + str(tiles))
        
#%%

