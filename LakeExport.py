#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
@author: simontopp
"""

import time
import ee
import os

## Initialize Earth Engine
ee.Initialize()

## Source necessary functions. We do this instead of 'import' because of EE quirk.
exec(open('GEE_pull_functions.py').read())

## Bring in EE Assets
## Deepest point (Chebyshev center) for CONUS PGDL Lakes
## DP Code available at https://zenodo.org/record/4136755#.X5d54pNKgUE and
## https://code.earthengine.google.com/8dac409b220bdfb051bb469bc5b3c708
dp = (ee.FeatureCollection('users/sntopp/USGS/PGDL_lakes_deepest_point')
  .filterMetadata('distance', "greater_than", 60))

## CONUS Boundary
us = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")\
    .filterMetadata('country_na', 'equals', 'United States')

## WRS Tiles in descending (daytime) mode for CONUS
wrs = ee.FeatureCollection('users/sntopp/wrs2_asc_desc')\
    .filterBounds(us)\
    .filterMetadata('MODE', 'equals', 'D')

## Run everything by path/row to speed up computation in EE.    
pr = wrs.aggregate_array('PR').getInfo()

## Bring in temp data
l8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
l7 = ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
era5 = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

## Standardize band names between the various collections and aggregate 
## them into one image collection
bn8 = ['ST_B10', 'ST_QA', 'QA_PIXEL']
bn57 = ['ST_B6', 'ST_QA', 'QA_PIXEL']
bns = ['temp','temp_qa','pixel_qa']
  
ls7 = l7.select(bn57, bns)
ls8 = l8.select(bn8, bns)

## Do coarse cloud filtering
ls = ee.ImageCollection(ls7.merge(ls8))\
    .filter(ee.Filter.lt('CLOUD_COVER', 50))\
    .filterBounds(us)  


## Set up a counter and a list to keep track of what's been done already
if not os.path.isfile('log.txt'):
    open('log.txt', 'x')

done = open('log.txt', 'r')
done = [x.strip() for x in done.readlines()]
done = list(map(int, done))
counter = len(done)

## In case something goofed, you should be able to just 
## re-run this chunk with the following line filtering out 
## what's already run. 
pr = [i for i in pr if i not in done]

for tiles in pr:
    tile = wrs.filterMetadata('PR', 'equals', tiles)
    
    lakes = dp.filterBounds(tile.geometry())\
        .map(dpBuff)
        
    stack = ls.filterBounds(tile.geometry().centroid())
    out = stack.map(RefPull).flatten().filterMetadata('cScore_clouds','less_than',.5)
    dataOut = ee.batch.Export.table.toDrive(collection = out,\
                                            description = str(tiles),\
                                            folder = 'EE_TempPull',\
                                            fileFormat = 'csv',\
                                            selectors = ["system:index", "areakm", "cScore_clouds", "CLOUD_COVER", 'SPACECRAFT_ID', 'DATE_ACQUIRED', "distance", "lake_mix_layer_temperature", "lake_total_layer_temperature", "pCount_water", "site_id", "temp", "temp_qa"])
    
    ## Check how many existing tasks are running and take a break if it's >15
    maximum_no_of_tasks(25, 120)
    ## Send next task.
    dataOut.start()
    counter = counter + 1
    done.append(tiles)
    f = open('log.txt', 'a')
    f.write(str(tiles) + '\n')
    f.close()

    print('done_' + str(counter) + '_' + str(tiles))
        


