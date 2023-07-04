#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Google Earth Engine Reflectance Pull Functions

@author: simontopp
"""
import ee
import time
import os

## Recreate the fmask based on Collection 2 Pixel QA band
def AddFmask(image):
    qa = image.select('pixel_qa')
    water = qa.bitwiseAnd(1 << 7)
    cloud = qa.bitwiseAnd(1 << 1).Or(qa.bitwiseAnd(1 << 2)).Or(qa.bitwiseAnd(1 << 3))
    snow = qa.bitwiseAnd(1 << 5)
    cloudshadow = qa.bitwiseAnd(1 << 4)

    fmask = (water.gt(0).rename('fmask')
             .where(snow.gt(0), ee.Image(3))
             .where(cloudshadow.gt(0), ee.Image(2))
             .where(cloud.gt(0), ee.Image(4))
             .updateMask(qa.gte(0)))
    ## mask the fmask so that it has the same footprint as the quality (BQA) band
    return image.addBands(fmask)


## Buffer the lake sites either 120 minutes or the distance to shore, whichever
## is smaller
def dpBuff(i):
    dist = i.get('distance')
    buffdist = ee.Number(dist).min(120)
    return i.buffer(buffdist)



## Set up the reflectance pull
def RefPull(image):
    f = AddFmask(image).select('fmask')
    water = f.eq(1).rename('water')
    clouds = f.gte(2).rename('clouds')

    era5match = (era5.filterDate(ee.Date(image.get('system:time_start')).update(None, None, None, None, 0,0)))

    era5out = (ee.Algorithms.If(era5match.size(),
                     ee.Image(era5match.first()).select('lake_mix_layer_temperature', 'lake_total_layer_temperature'),
                     ee.Image(0).rename('lake_mix_layer_temperature').addBands(ee.Image(0).rename('lake_total_layer_temperature')),
                     )
               )

    pixOut = (image.select('temp', 'temp_qa')
              .updateMask(water)
              .addBands(era5out)
              .addBands(clouds)
              .addBands(water))

    combinedReducer = (ee.Reducer.median().unweighted()
        .forEachBand(pixOut.select('temp', 'temp_qa', 'lake_mix_layer_temperature', 'lake_total_layer_temperature'))
        .combine(ee.Reducer.mean().unweighted().forEachBand(pixOut.select('clouds')), 'cScore_', False)
        .combine(ee.Reducer.count().unweighted().forEachBand(pixOut.select('water')), 'pCount_', False))

    def copyMeta(i):
        return i.copyProperties(image, ["CLOUD_COVER", 'SPACECRAFT_ID', 'system:index', 'DATE_ACQUIRED'])
    
    ## Remove geometries
    def removeGeo(i):
        return i.setGeometry(None)

    # Collect reflectance values, cloud score, and pixel counts
    lsout = pixOut.reduceRegions(lakes, combinedReducer, 30)

    out = lsout.map(removeGeo)
    out = out.map(copyMeta)

    return out


## Function for limiting the max number of tasks sent to
## earth engine at one time to avoid time out errors

def maximum_no_of_tasks(MaxNActive, waitingPeriod):
    ## maintain a maximum number of active tasks
    time.sleep(10)
    ## initialize submitting jobs
    ts = list(ee.batch.Task.list())

    NActive = 0
    for task in ts:
        if ('RUNNING' in str(task) or 'READY' in str(task)):
            NActive += 1
    ## wait if the number of current active tasks reach the maximum number
    ## defined in MaxNActive
    while (NActive >= MaxNActive):
        time.sleep(waitingPeriod) 
        ts = list(ee.batch.Task.list())
        NActive = 0
        for task in ts:
            if ('RUNNING' in str(task) or 'READY' in str(task)):
                NActive += 1
    return ()

