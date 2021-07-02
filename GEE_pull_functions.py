#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Google Earth Engine Reflectance Pull Functions
Created on Mon Apr  9 14:24:13 2018
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
    # mask the fmask so that it has the same footprint as the quality (BQA) band
    return image.addBands(fmask)


## Calculuate hillshadow to correct DWSE
def CalcHillShadows(image, geo):
    MergedDEM = ee.Image("users/eeProject/MERIT").clip(geo.buffer(3000))
    hillShadow = (ee.Terrain.hillShadow(MergedDEM, ee.Number(image.get('SOLAR_AZIMUTH_ANGLE')),
                                        ee.Number(90).subtract(image.get('SOLAR_ZENITH_ANGLE')), 30).rename(
        ['hillShadow']))
    return hillShadow


## Buffer the lake sites
def dpBuff(i):
    dist = i.get('distance')
    buffdist = ee.Number(dist).min(120)
    return i.buffer(buffdist)


## Remove geometries
def removeGeo(i):
    return i.setGeometry(None)


## Create water mask and extract lake medians

## Set up the reflectance pull
def RefPull(image):
    f = AddFmask(image).select('fmask')
    water = f.eq(1).rename('water')
    clouds = f.gte(2).rename('clouds')
    #hs = CalcHillShadows(image, tile.geometry()).select('hillShadow')
    #era5match = (ee.Image(era5.filterDate(ee.Date(image.get('system:time_start')).update(minute = 0, second = 0))
    #             .first()).select('lake_mix_layer_temperature', 'lake_total_layer_temperature'))
    era5match = ee.Image(0).rename('lake_mix_layer_temperature').addBands(ee.Image(1).rename('lake_total_layer_temperature'))
    pixOut = (image
              .addBands(era5match)
              .updateMask(water)
              .addBands(clouds)
              .addBands(water))

    combinedReducer = (ee.Reducer.median().unweighted()
        .forEachBand(pixOut.select('temp', 'temp_qa', 'lake_mix_layer_temperature', 'lake_total_layer_temperature'))
        .combine(ee.Reducer.mean().unweighted().forEachBand(pixOut.select('clouds')), 'cScore_', False)
        .combine(ee.Reducer.count().unweighted().forEachBand(pixOut.select('water')), 'pCount_', False))


    # Collect median reflectance and occurance values
    # Make a cloud score, and get the water pixel count
    lsout = pixOut.reduceRegions(lakes, combinedReducer, 30)

    out = lsout.map(removeGeo)

    return out


##Function for limiting the max number of tasks sent to
# earth engine at one time to avoid time out errors

def maximum_no_of_tasks(MaxNActive, waitingPeriod):
    ##maintain a maximum number of active tasks
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
        time.sleep(waitingPeriod)  # if reach or over maximum no. of active tasks, wait for 2min and check again
        ts = list(ee.batch.Task.list())
        NActive = 0
        for task in ts:
            if ('RUNNING' in str(task) or 'READY' in str(task)):
                NActive += 1
    return ()

