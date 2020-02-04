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
def polygon_to_gridCell_hashSet(poly, cell_size_degrees = 0.25):
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

## Script
## Get data
source_bucket_dir = "s3a://major-us-ports-csv/"
source_file_name = "geoPorts_v2.parquet"
ports = sqlContext.read.parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag

source_bucket_dir = "s3a://ais-ship-pings-parquet/"
source_file_name = "pings.parquet/Year=2017/Month=1"
pings = sqlContext.read.parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag
# pings = sqlContext.read('magellan').parquet(source_bucket_dir + source_file_name) # Fastest by an order of mag

# Invoke Function for geoHashing Ports:
cell_size_degrees = 1
# ports = ports.withColumn("GRID_CELLS_LAT", polygon_to_gridCell_hashSet_udf(ports.POLYGON10KM, psql.lit("LAT"), psql.lit(cell_size_degrees)))
# ports = ports.withColumn("GRID_CELLS_LON", polygon_to_gridCell_hashSet_udf(ports.POLYGON10KM, psql.lit("LON"), psql.lit(cell_size_degrees)))
# v2
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

# port_geoHash_cellsLat = ports.select(
# 		ports.PORT_NAME, \
#         psql.split("GRID_CELLS_LAT", ", ").alias("GRID_CELLS_LAT"), \
#         psql.posexplode(psql.split("GRID_CELLS_LAT", ", ")).alias("pos", "val") \
#     )
# port_geoHash_cellsLon = ports.select(
# 		ports.PORT_NAME, \
#         psql.split("GRID_CELLS_LON", ", ").alias("GRID_CELLS_LON"), \
#         psql.posexplode(psql.split("GRID_CELLS_LON", ", ")).alias("pos", "val") \
#     )

# Convert cells to integer
# port_geoHash_cellsLat = port_geoHash_cellsLat.withColumn('val_int', psql.regexp_extract('val', '(-)*[0-9]+', 0).cast('integer'))
# ports_unique_lat_cells = port_geoHash_cellsLat.select('val_int').distinct()
# ports_unique_lat_cells = [int(row.val_int) for row in ports_unique_lat_cells.collect()]

# port_geoHash_cellsLon = port_geoHash_cellsLon.withColumn('val_int', psql.regexp_extract('val', '(-)*[0-9]+', 0).cast('integer'))
# ports_unique_lon_cells = port_geoHash_cellsLon.select('val_int').distinct()
# ports_unique_lon_cells = [int(row.val_int) for row in ports_unique_lon_cells.collect()]

# This is an overly long data structure and could be a pinch point- a matrix (wide) form would be better with cell vallues = PORT_NAME
# ports_geoHash = port_geoHash_cellsLon.selectExpr('PORT_NAME', 'val_int as val_int_Lon').join(port_geoHash_cellsLat.selectExpr('PORT_NAME', 'val_int as val_int_Lat'), on = 'PORT_NAME')
# ports_geoHash = ports_geoHash.join(ports.select('PORT_NAME', 'POLYGON10KM'), on = 'PORT_NAME', how = 'left')
# ports_geoHash = ports_geoHash.withColumn('LON_LAT', lonLatString_to_geoPoint_udf(ports_geoHash.val_int_Lon, ports_geoHash.val_int_Lat)).select('PORT_NAME', 'LON_LAT', 'val_int_Lon').distinct()

# GeoHash Pings:
pings = pings.withColumn("GRID_X", psql.floor(pings.LON.cast('INT')/cell_size_degrees))
pings = pings.withColumn("GRID_Y", psql.floor(pings.LAT.cast('INT')/cell_size_degrees))

# pingsB = pings.filter(pings.Status == 'moored').limit(250) # for testing
# jpings.filter(jpings.PORT_NAME.isNotNull()).count() # for testing
pings = pings.join(psql.broadcast(ports_geoHash.select("PORT_NAME", "POLYGON10KM", "LON_CELL", "LAT_CELL")), on = (pingsB.GRID_X == ports_geoHash.LON_CELL) & (pingsB.GRID_Y == ports_geoHash.LAT_CELL), how = 'left')




26.74790|-80.05106

# Add PORT column, labeling 'underway' unless matches geoHash:
pings = pings.withColumn("PORT", psql.when(pings.GRID_X.isin(ports_unique_lon_cells) & pings.GRID_Y.isin(ports_unique_lat_cells), "Port").otherwise("Underway")) #psql.lit("Underway"))
# take out geoHash matching pings.
pings_filt = pings.filter(pings.GRID_X.isin(ports_unique_lon_cells) & pings.GRID_Y.isin(ports_unique_lat_cells))
# lookup ports for ping list.
pings_filt = pings_filt.withColumn("LON_LAT", lonLatString_to_geoPoint_udf(pings.LON, pings.LAT))



def polygon_contains_point(polygonWKT, pointWKT):
   sh_polygon = shapely.wkt.loads(polygonWKT)
   sh_point = shapely.wkt.loads(pointWKT)
   return sh_polygon.contains(sh_point)

polygon_contains_point_udf = psql.udf(polygon_contains_point)

def match_port(ping_point, grid_LON, grid_LAT):
	# This does not work in spark SQL context!!!!!
	# testing vals:
	# ping_point = pings_filt.select("LON_LAT").take(1)[0][0]
	# grid_LON = pings_filt.select("GRID_X").take(1)[0][0]
	# grid_LAT = pings_filt.select("GRID_Y").take(1)[0][0]
	# func:
	try:
		LAT_potential_ports = port_geoHash_cellsLat.filter(port_geoHash_cellsLat.val_int == grid_LAT)
		LON_potential_ports = port_geoHash_cellsLon.filter(port_geoHash_cellsLon.val_int == grid_LON)
		# print(LAT_potential_ports)
		possible_ports = LAT_potential_ports.join(LON_potential_ports, on = 'PORT_NAME', how = 'inner')
		possible_ports = possible_ports.join(ports.select("PORT_NAME", "POLYGON10KM"), on = "PORT_NAME", how = "left")
		# return(possible_ports.show())
	except:
		print("ERROR")

		if possible_ports.count() > 0:
			possible_ports = possible_ports.withColumn("PING_MATCH", polygon_contains_point_udf(possible_ports.POLYGON10KM, psql.lit(ping_point)))
			matching_port = possible_ports.select("PORT_NAME", "PING_MATCH").where(possible_ports.PING_MATCH == 'true').collect()
			if len(matching_port) > 1:
				return("ERROR: Multiple Matches")
			elif len(matching_port) == 1:
				return(matching_port.limit(1).select("PORT_NAME").collect()[0][0])
		else:
			return("Underway")
	except:
		print("NO POTENTIAL PORTS")

def spark_match_port(ping_point, grid_LON, grid_LAT):
	# This does not work in spark SQL context!!!!!
	# testing vals:
	# ping_point = pings_filt.select("LON_LAT").take(1)[0][0]
	# grid_LON = pings_filt.select("GRID_X").take(1)[0][0]
	# grid_LAT = pings_filt.select("GRID_Y").take(1)[0][0]
	# func:
	try:
		LAT_potential_ports = port_geoHash_cellsLat.filter(port_geoHash_cellsLat.val_int == grid_LAT)
		LON_potential_ports = port_geoHash_cellsLon.filter(port_geoHash_cellsLon.val_int == grid_LON)
		# print(LAT_potential_ports)
		possible_ports = LAT_potential_ports.join(LON_potential_ports, on = 'PORT_NAME', how = 'inner')
		possible_ports = possible_ports.join(ports.select("PORT_NAME", "POLYGON10KM"), on = "PORT_NAME", how = "left")
		
		ports_geoHash.toPandas().filter()
		# return(possible_ports.show())
	except:
		print("ERROR")

		if possible_ports.count() > 0:
			possible_ports = possible_ports.withColumn("PING_MATCH", polygon_contains_point_udf(possible_ports.POLYGON10KM, psql.lit(ping_point)))
			matching_port = possible_ports.select("PORT_NAME", "PING_MATCH").where(possible_ports.PING_MATCH == 'true').collect()
			if len(matching_port) > 1:
				return("ERROR: Multiple Matches")
			elif len(matching_port) == 1:
				return(matching_port.limit(1).select("PORT_NAME").collect()[0][0])
		else:
			return("Underway")
	except:
		print("NO POTENTIAL PORTS")

match_port_udf = psql.udf(match_port)

pings_filta = pings_filt.limit(10)

ports_geoHash

pings_filta.withColumn("PORT_MATCH", polygon_contains_point_udf(psql.lit(ports_geoHash.filter(ports_geoHash.val_int == pings_filta.GRID_X & ports_geoHash.val_int_Lat == pings_filta.GRID_Y)), pings_filta.LON_LAT))
pings_filta.withColumn("PORT_MATCH", psql.lit(ports_geoHash.filter(psql.col("val_int") == pings_filta.GRID_X ))& psql.col("val_int_Lat") == pings_filta.GRID_Y)))
pings_filta.withColumn("PORT_MATCH", polygon_contains_point_udf(ports_geoHash.filter(ports_geoHash.val_int == pings_filta.GRID_X), pings_filta.LON_LAT))


	, pings_filt1.GRID_Y))

pings_filt1.withColumn("PORT_MATCH", match_port_udf(pings_filt1.LON_LAT, pings_filt1.GRID_X, pings_filt1.GRID_Y))


polygon_contains_point( \
ports.filter(ports.PORT_NAME == "Port Arthur, TX").select('POLYGON10KM').collect()[0][0] , \
ping_point)

possible_ports.withColumn("PING_MATCH", polygon_contains_point(ports.filter(ports.PORT_NAME == "Port Arthur, TX"), ping_point))
possible_ports.withColumn("PING_MATCH", ports.filter(ports.PORT_NAME == "Port Arthur, TX"))
	ports.filter()

