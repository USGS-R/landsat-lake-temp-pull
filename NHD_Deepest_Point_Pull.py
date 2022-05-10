import ee
import math
import time
ee.Initialize()

## Deepest point calculation adapted from Xiao Yang
### https: // doi.org / 10.5281 / zenodo.4136754
### Functions
def get_scale(polygon):
    radius = polygon.get('areasqkm')
    radius = ee.Number(radius).divide(math.pi).sqrt().multiply(1000)
    return radius.divide(20)


def getUTMProj(lon, lat):
    # see
    # https: // apollomapping.com / blog / gtm - finding - a - utm - zone - number - easily and
    # https: // sis.apache.org / faq.html
    utmCode = ee.Number(lon).add(180).divide(6).ceil().int()
    output = ee.Algorithms.If(ee.Number(lat).gte(0),
                              ee.String('EPSG:326').cat(utmCode.format('%02d')),
                              ee.String('EPSG:327').cat(utmCode.format('%02d')))
    return output

def GetLakeCenters(polygon):

    ## Calculate both the deepest point an centroid
    ## for the inpout polygon ( or multipolygon)
    ## for each input, export geometries for both centroid and deepest point and their distance to shore.
    scale = get_scale(polygon)
    geo = polygon.geometry()
    ct = geo.centroid(scale)
    utmCode = getUTMProj(ct.coordinates().getNumber(0), ct.coordinates().getNumber(1))

    polygonImg = ee.Image.constant(1).toByte().paint(ee.FeatureCollection(ee.Feature(geo, None)), 0)

    dist = polygonImg.fastDistanceTransform(2056).updateMask(polygonImg.Not()).sqrt().reproject(utmCode, None, scale).multiply(scale) # convert unit from pixel to meter

    # dist = (polygonImg.fastDistanceTransform(2056).updateMask(polygonImg.not ())
    # .sqrt().reproject('EPSG:4326', None, scale).multiply(scale)  # convert unit from pixel to meter

    maxDistance = (dist.reduceRegion(
        reducer=ee.Reducer.max(),
        geometry=geo,
        scale=scale,
        bestEffort=True,
        tileScale=1
    ).getNumber('distance'))

    outputDp = (ee.Feature(dist.addBands(ee.Image.pixelLonLat()).updateMask(dist.gte(maxDistance))
                          .sample(geo, scale).first()))

    dp = ee.Geometry.Point([outputDp.get('longitude'), outputDp.get('latitude')])

    regions = ee.FeatureCollection([ee.Feature(dp, {'type': 'dp'})])

    output = dist.sampleRegions(
        collection=regions,
        properties=['type'],
        scale=scale,
        tileScale=1,
        geometries=True)

    return (ee.Feature(output.first()).copyProperties(polygon,['permanent','areasqkm']))

def buff_dp(dp):
    return dp.buffer(dp.getNumber('distance'))


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


assets_parent = ee.data.listAssets({'parent': 'projects/earthengine-legacy/assets/projects/sat-io/open-datasets/NHD'})['assets']
assets_parent = assets_parent[0:2]
for i in range(len(assets_parent)):
    state_asset = assets_parent[i]['id']
    assets_state = (ee.FeatureCollection(f"{state_asset}/NHDWaterbody")
    .filter(ee.Filter.gte('areasqkm',0.0001))
    .filter(ee.Filter.inList('ftype',[361,436,390])))

    dp = ee.FeatureCollection(assets_state).map(GetLakeCenters)

    dataOut = ee.batch.Export.table.toAsset(collection=dp,description=state_asset.split('/')[-1],assetId=f"projects/earthengine-legacy/assets/users/sntopp/NHD/DeepestPoint/{state_asset.split('/')[-1]}")

    ## Check how many existing tasks are running and take a break if it's >15
    maximum_no_of_tasks(10, 240)
    ## Send next task.
    dataOut.start()
    print(state_asset.split('/')[-1])

#f"projects/earthengine-legacy/assets/users/sntopp/NHD/{state_asset.split('/')[-1]}/NHDDeepestPoint"


