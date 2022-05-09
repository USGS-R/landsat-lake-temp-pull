import ee
import math
ee.Initialize()

## Deepest point calculation adapted from Xiao Yang
### https: // doi.org / 10.5281 / zenodo.4136754
#
# table_filt = nhdhr.filter(ee.Filter.gte('AREASQK', .01))
# print(table_filt.size())
# print(table_filt)
#
# Map.addLayer(nhdhr)
# // Map.centerObject(fail_point, 12)
# // print(blah)


def get_scale(polygon):
    radius = polygon.get('areasqkm').getInfo()
    radius = ee.Number(radius).divide(math.pi).sqrt().multiply(1000)
    return radius.divide(7)


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

    return (ee.Feature(output.first()).copyProperties(polygon))

assets_parent = ee.data.listAssets({'parent': 'projects/earthengine-legacy/assets/projects/sat-io/open-datasets/NHD'})['assets']


assets_state =  (ee.FeatureCollection(f"{assets_parent[1]['id']}/NHDWaterbody")
    .filter(ee.Filter.gte('areasqkm',0.0001))
    .filter(ee.Filter.inList('ftype',[361,436,390])))

dp = GetLakeCenters(ee.Feature(assets_state.first()))

dp_buff = test.map(function(f)
{
return (f.buffer(f.getNumber('distance')))})

var


colorDp = {color: 'cyan'}
Map.addLayer(nhdhr.filterBounds(geometry))
Map.addLayer(dp_buff, colorDp, 'DpBuffer', true, 0.5)
Map.addLayer(test, colorDp, 'Deepest Point', true)
Map.centerObject(geometry)

var
dp_out = nhdhr.filter(ee.Filter.gte('AREASQK', 0.001))
    .filter(ee.Filter.lt('AREASQK', 0.01)).map(GetLakeCenters)
Export.table.toAsset(dp_out, 'USGS/NHD_hr_dp_f361_436_390_gt1hectare')


