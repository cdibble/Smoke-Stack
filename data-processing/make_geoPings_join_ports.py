# make_geoPings_join_Ports.py
#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql import Row
from pyspark.sql.types import *
# import boto3
import botocore
import datetime
import os
import rasterio
from shapely.geometry import Point, Polygon

import shapely
import geopyspark
import geopandas 
from shapely.ops import transform
import geopyspark
import pyproj # projection strings for geospatial
from functools import partial
##
import math
## Spark Session
conf = SparkConf().setAppName("AIS_Extract") #.setMaster(spark://10.0.0.7:7077)
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
## Functions
	# NOTE: This should be moved to a different file and passed as functions to all scripts that use it
# def lonLatString_to_geoPoint_shape(lon, lat):
# 	points = Point(float(lon), float(lat))
# 	return points
# Register UDF
# lonLatString_to_geoPoint_shape_udf = psql.udf(lonLatString_to_geoPoint_shape)

# def geo_hash(LON_LAT):
# 	points = shapely.wkt.loads(LON_LAT)
# 	cell_1 = 0.5
# 	cell_2 = cell_1 * 0.5
# 	spark.sql(f"select cast(floor(points.x/{cell_1}) as INT) as q, cast(floor(points.y/{cell_1}) as INT) as r from df_with_LON_LAT")\
# 		.registerTempTable("QR")
# 	QR.first()
# 	spark.sql(f"select q*{cell_1}+{cell_2} as x,r*{cell_1}+{cell_2} as y,count(1) as pop from QR group by q,r having pop > 100").show()

# geo_hash_udf = psql.udf(geo_hash)
# ports.withColumn('geoHash', geo_hash_udf(ports.LON_LAT))
def lonLatString_to_geoPoint(lon, lat):
	points = Point(float(lon), float(lat)).wkt
	return points
# Register UDF
lonLatString_to_geoPoint_udf = psql.udf(lonLatString_to_geoPoint)

poly = ports.select("POLYGON10KM").take(1)[0][0]
# Get polygon components and compute grid cell set 
def polygon_to_gridCell_hashSet(poly, cell_size_degrees):
	# SOMETHING WRONG WITH THIS!!
	poly = shapely.wkt.loads(poly)
	lons = poly.exterior.coords.xy[0]
	lats = poly.exterior.coords.xy[1]
	# # version 1.
	# lon_grid_cells = list(set([math.floor(lons / cell_size_degrees) for lons in lons]))
	# lat_grid_cells = list(set([math.floor(lats / cell_size_degrees) for lats in lats]))
	# if out == "LON":
	# 	return grids[0]
	# elif out == "LAT":
	# 	return grids[1]
	# version 2
	lon_grid_cells = list([math.floor(lons / cell_size_degrees) for lons in lons])
	lat_grid_cells = list([math.floor(lats / cell_size_degrees) for lats in lats])
	points = [Point(x, y).wkt for x, y in zip(lon_grid_cells, lat_grid_cells)]
	grids = list(set(points))
	return(grids)
	
# polygon_to_gridCell_hashSet(poly = ports.select("POLYGON10KM").take(1)[0][0]) # for Testing.
polygon_to_gridCell_hashSet_udf = psql.udf(polygon_to_gridCell_hashSet) # default is string type output... change to array?

def polygon_contains_point(polygonWKT, pointWKT):
   sh_polygon = shapely.wkt.loads(polygonWKT)
   sh_point = shapely.wkt.loads(pointWKT)
   return sh_polygon.contains(sh_point)

polygon_contains_point_udf = psql.udf(polygon_contains_point)

## Script
## Get data
source_bucket_dir = "s3a://major-us-ports-csv/"
source_file_name = "geoPorts_v2.parquet"
ports = sqlContext.read.parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag

source_bucket_dir = "s3a://ais-ship-pings-parquet/"
source_file_name = "pings.parquet/Year=2017/Month=1"
pings = sqlContext.read.parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag
# pings.count() = 218,689,038  

# Invoke Function for geoHashing Ports:
# cell_size_degrees = 0.025 gives pings.count() 1616539
# cell_size_degrees = 0.05 gives pings.count() 4365613
# cell_size_degrees = 0.125 gives pings.count() 42572446
# cell_size_degrees = 0.25 gives pings.count() 78621860  235,939,607 
# cell_size_degrees = 0.5 gives pings.count() 127822554
# cell_size_degrees = 1 gives pings.count() 345081495
# cell_size_degrees = 2 gives pings.count() 664358528
cell_size_degrees = 0.25
ports = ports.withColumn("GRID_CELLS", polygon_to_gridCell_hashSet_udf(ports.POLYGON10KM, psql.lit(cell_size_degrees)))
ports = ports.withColumn("GRID_CELLS", psql.expr("substring(GRID_CELLS, 2, (length(GRID_CELLS)-1))"))

# Explote geoHash cells from list types to (one row per cell) to get complete set of port geoHash cells
ports_geoHash = ports.select(
		ports.PORT_NAME, \
		ports.POLYGON10KM, \
		ports.LON_LAT, \
        psql.split("GRID_CELLS", ", ").alias("GRID_CELLS"), \
        psql.posexplode(psql.split("GRID_CELLS", ", ")).alias("pos", "val") \
    )
# Take exploded points, separate into integer lat and lon for joining
ports_geoHash = ports_geoHash.withColumn('GRID_POINTS', psql.regexp_extract('val', '(-)*[0-9]+ (-)*[0-9]+', 0)).withColumn('LON_CELL', psql.split('GRID_POINTS', ' ').getItem(0).cast("INT")).withColumn('LAT_CELL', psql.split('GRID_POINTS', " ").getItem(1).cast("INT"))

# GeoHash Pings:
pings = pings.withColumn("GRID_X", psql.floor(pings.LON.cast('INT')/cell_size_degrees))
pings = pings.withColumn("GRID_Y", psql.floor(pings.LAT.cast('INT')/cell_size_degrees))

# pingsB = pings.filter(pings.Status == 'moored').limit(250) # for testing
# jpings.filter(jpings.PORT_NAME.isNotNull()).count() # for testing
pings = pings.join(psql.broadcast(ports_geoHash.select("PORT_NAME", "POLYGON10KM", "LON_CELL", "LAT_CELL")), on = (pings.GRID_X == ports_geoHash.LON_CELL) & (pings.GRID_Y == ports_geoHash.LAT_CELL), how = 'left_outer')
# Now test those that matched
pings_to_test = pings.filter(pings.PORT_NAME.isNotNull())
pings_to_test = pings.filter(psql.isnull(psql.col(pings.PORT_NAME)))

pings_to_test.count()

