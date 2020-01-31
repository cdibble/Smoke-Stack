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
	# needs to be able to take a list of points
	point = shapely.wkt.loads(point) # if you want to accept string as input
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
	# print(type(polygon_wgs84.wkt))
	return(polygon_wgs84.wkt)
	# print(mapping(polygon_wgs84))
	# print(json.dumps(mapping(buffer_wgs84)))
## Test geoPoint_buffer_to_polygon
# geoPoint_buffer_to_polygon(list(geoPorts.select('LON_LAT').first())[0])
# Register UDF
geoPoint_buffer_to_polygon_udf = psql.udf(geoPoint_buffer_to_polygon, StringType())

def lonLatString_to_geoPoint(lon, lat):
	points = Point(float(lon), float(lat)).wkt
	return points
# Register UDF
lonLatString_to_geoPoint_udf = psql.udf(lonLatString_to_geoPoint)

def write_df_to_parquet(spark, df_i):
		# Write Parquet to S3 
		target_bucket_dir = "s3a://major-us-ports-csv/"
		target_file_name = 'geoPorts_v2.parquet'
		df_i.write.mode("append").format('parquet').option('compression', 'snappy').save(target_bucket_dir + target_file_name)
## Get Data -> Munge Spatial Points -> Add Polygon with Buffer 
# client.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
ports = sqlContext.read.csv("s3a://major-us-ports-csv/" + 'Major_Ports.csv', header = 'True')
# Version 2.
geoPorts = ports.withColumn('LON_LAT', lonLatString_to_geoPoint_udf(ports.X, ports.Y))
polyPorts = geoPorts.withColumn('POLYGON10KM', geoPoint_buffer_to_polygon_udf(geoPorts.LON_LAT))


# Version 1. Slow.
# lons = [float(x[0]) for x in ports.select('X').collect()] # get lons as list of floats
# lats = [float(x[0]) for x in ports.select('Y').collect()] # get lats as list of floats
# geoPoints = [Point(x, y) for x, y in zip(lons, lats)] # turn lons, lats into Point()
# geoPolys_wkt = [geoPoint_buffer_to_polygon(point) for point in geoPoints]
# geoPoints_wkt = [geoPoints[i].wkt for i in range(len(geoPoints))]
# # spark DataFrame version (use this to avoid conversion to RDD and back)
# # Convert geoPoints to data frame with index column
# R = Row('index', 'LON_LAT')
# spark_geoPoints = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(geoPoints_wkt)])
# # Convert geoPoly to data frame with index column
# R = Row('index', 'POLYGON10KM')
# spark_geoPolys = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(geoPolys_wkt)])
# # geoPoints_index = index.zip(spark_geoPoints)
# ports_index = ports.withColumn('index', psql.monotonically_increasing_id())
# # Join ports_index and geoPoints_index using index
# geoPorts = ports_index.join(spark_geoPoints, "index")
# polyPorts = geoPorts.join(spark_geoPolys, "index")

## Write to Parquet
write_df_to_parquet(sc, polyPorts)
