# ports_to_geoSpark.py
#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
import boto3 
import botocore
import datetime
import os
import rasterio
from shapely.geometry import Point, Polygon
import geopyspark
from geopandas import GeoDataFrame

conf = SparkConf().setAppName("AIS_Extract") #.setMaster(spark://10.0.0.7:7077)
sc = SparkContext(conf=conf)

def write_df_to_parquet(spark, df_i):
		# Write Parquet to S3 
		target_bucket_dir = "s3a://major-us-ports-csv/"
		target_file_name = 'geoPorts.parquet'
		df_i.write.mode("append").format('parquet').option('compression', 'snappy').save(target_bucket_dir + target_file_name)

s3 = boto3.resource('s3') # TODO: Initialize is expensive. Can I do all of the client. operations below with this resrouce?
ais_bucket = s3.Bucket('major-us-ports-csv')
files_in_bucket = list(ais_bucket.objects.all())
client = boto3.client('s3')

# client.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
ports = spark.read.csv("s3a://major-us-ports-csv/" + 'Major_Ports.csv', header = 'True')
port_column_names = ports.columns
lons = [float(x[0]) for x in ports.select('X').collect()] # get lons as list of floats
lats = [float(x[0]) for x in ports.select('Y').collect()] # get lats as list of floats
geoPoints = [Point(x, y) for x, y in zip(lons, lats)] # turn lons, lats into Point()
geoPoints = [geoPoints[i].wkt for i in range(len(geoPoints))]
ports = ports.rdd
index = sc.parallelize(range(0, len(geoPoints)), ports.getNumPartitions())
geoPoints = sc.parallelize(geoPoints, ports.getNumPartitions())
geoPoints_index = index.zip(geoPoints)
ports_index = index.zip(ports)
# Join ports_index and geoPoints_index using index
assert(ports_index.count() == geoPoints_index.count())
# Join ports and geoPoints
geoPorts = ports_index.join(geoPoints_index).map(lambda row: [row[0], *list(row[1][0]) , row[1][1]])
geoPorts = geoPorts.toDF(['index', *port_column_names, 'LON_LAT']).drop('X', 'Y')

## TODO: Turn Points into Circle Geom

write_df_to_parquet(sc, geoPorts)