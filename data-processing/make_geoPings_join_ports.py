# make_geoPings_join_Ports.py
#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql.types import *
import boto3 
import botocore
import datetime
import os
import rasterio
from shapely.geometry import Point, Polygon
import geopyspark
import geopandas 

conf = SparkConf().setAppName("AIS_Extract") #.setMaster(spark://10.0.0.7:7077)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

source_bucket_dir = "s3a://ais-ship-pings-parquet/"
source_file_name = "pings.parquet/Year=2017/Month=1"
pings = sqlContext.read.parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag

pings = pings.withColumn("LON", pings.lon.cast(FloatType))
df.select( df("year").cast(IntegerType).as("year"), ... )

geoPoints = [Point(x, y) for x, y in zip(pings.select('LON'), pings.select('LAT'))] # turn lons, lats into Point()
geoPoints = [geoPoints[i].wkt for i in range(len(geoPoints))]

# pingsDF = pings.toPandas()
# geoPings = geopandas.GeoDataFrame(
#     pings, geometry=geopandas.points_from_xy(pings.select('LON'), pings.select('LAT')))
