# ports_to_geoSpark.py
#!/usr/bin/env python3
## Imports
## pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql import Row
from pyspark.sql.types import *
import boto3 
import botocore
## geospatial
import rasterio
from shapely.geometry import Point, Polygon, mapping
import shapely.wkt
from shapely.ops import transform
import geopyspark
from geopandas import GeoDataFrame
import pyproj
## utility
# import json
from functools import partial
import datetime
import os

## Spark Session
conf = SparkConf().setAppName("AIS_Extract") #.setMaster(spark://10.0.0.7:7077)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
## Functions
def get_meth(object):
	object_methods = [method_name for method_name in dir(object) if callable(getattr(object, method_name))]
	return object_methods
def geoPoint_buffer_to_polygon(point):
	# this function owes a lot to 'bugmenot123': # https://gis.stackexchange.com/questions/268250/generating-polygon-representing-rough-100km-circle-around-latitude-longitude-poi/268277#268277
	# point = shapely.wkt.loads(point) # if you want to accept string as input
	try:
		point.__getattribute__('x')
	except AttributeError:
		print("Error: Input point must be a shapely Point(lon, lat) object")
	# create aeqd projection, which will allow a buffer based on the location string.
	local_azimuthal_projection = f"+proj=aeqd +R=6371000 +units=m +lat_0={point.y} +lon_0={point.x}" # projection string
	# Build partial functions with pyproj.transform that can be passed to shapely.ops.transform to re-project a spatila object
	# Need a function to go from wgs84 (lat/lon) to aeqd and a function to do the reverse
	wgs84_to_aeqd = partial(
	    pyproj.transform,
	    pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
	    pyproj.Proj(local_azimuthal_projection),
	)
	aeqd_to_wgs84 = partial(
	    pyproj.transform,
	    pyproj.Proj(local_azimuthal_projection),
	    pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
	)
	# transform point to local aeqd
	point_aeqd = transform(wgs84_to_aeqd, point)
	aeqd_buffered_to_polygon = point_aeqd.buffer(20_000)
	polygon_wgs84 = transform(aeqd_to_wgs84, aeqd_buffered_to_polygon)
	return(polygon_wgs84.wkt)
	# print(mapping(polygon_wgs84))
	# print(json.dumps(mapping(buffer_wgs84)))
## Test geoPoint_buffer_to_polygon
# geoPoint_buffer_to_polygon(list(geoPorts.select('LON_LAT').first())[0])

def lonLatString_to_geoPoint(lon, lat):
	 # sqlContext.createDataFrame([R(i, x) for i, x in enumerate(geoPoints_wkt)])
	points = Point(float(lon), float(lat)).wkt
	return points

lonLatString_to_geoPoint_udf = psql.udf(lonLatString_to_geoPoint)

# map(lambda x: write_csv_parquet_to_s3(parse_csv(x, bucket_in = 'ais-ship-data-raw').collect(), file_name = 'pings_from_csv.parquet'))

def write_df_to_parquet(spark, df_i):
		# Write Parquet to S3 
		target_bucket_dir = "s3a://major-us-ports-csv/"
		target_file_name = 'geoPorts.parquet'
		df_i.write.mode("append").format('parquet').option('compression', 'snappy').save(target_bucket_dir + target_file_name)
## Get Data -> Munge Spatial Points -> Add Polygon with Buffer 
# client.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
ports = sqlContext.read.csv("s3a://major-us-ports-csv/" + 'Major_Ports.csv', header = 'True')
lons = [float(x[0]) for x in ports.select('X').collect()] # get lons as list of floats
lats = [float(x[0]) for x in ports.select('Y').collect()] # get lats as list of floats
gePorts = ports.withColumn('LON_LAT', lonLatString_to_geoPoint_udf(ports.X, ports.Y))
geoPoints = [Point(x, y) for x, y in zip(lons, lats)] # turn lons, lats into Point()
geoPolys_wkt = [geoPoint_buffer_to_polygon(point) for point in geoPoints]
geoPoints_wkt = [geoPoints[i].wkt for i in range(len(geoPoints))]
# spark DataFrame version (use this to avoid conversion to RDD and back)
# Convert geoPoints to data frame with index column
R = Row('index', 'LON_LAT')
spark_geoPoints = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(geoPoints_wkt)])
# Convert geoPoly to data frame with index column
R = Row('index', 'POLYGON10KM')
spark_geoPolys = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(geoPolys_wkt)])
# geoPoints_index = index.zip(spark_geoPoints)
ports_index = ports.withColumn('index', psql.monotonically_increasing_id())
# Join ports_index and geoPoints_index using index
geoPorts = ports_index.join(spark_geoPoints, "index")
polyPorts = geoPorts.join(spark_geoPolys, "index")

## Write to Parquet
write_df_to_parquet(sc, polyPorts)





[POINT (-90.6590509998076 29.57907000004379),
POINT (-90.61793999968185 30.0334500003297),
POINT (-91.19934000015607 30.42292000007941),
POINT (-91.43073200003357 31.54723799990148),
POINT (-90.90055999978968 32.33469999989674),
POINT (-91.15361000028163 32.79471999991519),
POINT (-91.12686900031129 33.35923199977935),
POINT (-89.68744700010141 29.48000399978501),
POINT (-90.0852559999848 29.91414000013753),
POINT (-89.08533000044253 30.35215999978056),
POINT (-88.55879000028344 30.34802000019582),
POINT (-88.0411300003198 30.72527000000661),
POINT (-84.19926000026548 30.19009000021664),
POINT (-82.60270999960073 27.65999999979972),
POINT (-82.52235000032071 27.7853400001281),
POINT (-80.18164199992312 25.78286200018114),
POINT (-80.11780100002852 26.09339200007157),
POINT (-80.05266999962299 26.76903999983693),
POINT (-80.60815000010946 28.41409000006081),
POINT (-81.66512500027817 30.32275699981061),
POINT (-81.49987999988412 31.158559999745),
POINT (-81.0953819997664 32.08471100021607),
POINT (-79.92159500012362 32.78878099996054),
POINT (-95.74001999966019 36.21820000000334),
POINT (-94.60762000032766 39.11500000019358),
POINT (-93.06190000040833 44.95156999980364),
POINT (-91.0220600004061 33.81355100009842),
POINT (-90.58322999991033 34.52186000036581)] 