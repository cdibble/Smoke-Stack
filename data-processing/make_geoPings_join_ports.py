# make_geoPings_join_Ports.py
#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql.functions import pandas_udf, PandasUDFType, lag
from pyspark.sql.window import Window
import pyarrow # for pandas_udf functionality
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
from numpy import timedelta64 as np
## Spark Session
conf = SparkConf().setAppName("PortJoin_VisitIndex") #.setMaster(spark://10.0.0.7:7077)
# conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")

## Functions
	# NOTE: This should be moved to a different file and passed as functions to all scripts that use it
def lonLatString_to_geoPoint(lon, lat):
	points = Point(float(lon), float(lat)).wkt
	return points
# Register UDF
lonLatString_to_geoPoint_udf = psql.udf(lonLatString_to_geoPoint)

poly = 'POLYGON ((-122.1713902164701 37.80644595985081, -122.1725404834023 37.78881835827151, -122.1758704795785 37.77136466365747, -122.1813466437318 37.75425278095299, -122.1889148667078 37.73764721204931, -122.1985010647773 37.72170748303641, -122.2100119385397 37.7065866231559, -122.2233359084126 37.6924297095073, -122.2383442163353 37.67937249080907, -122.2548921820902 37.66754010266531, -122.272820601571 37.6570458858488, -122.2919572733889 37.64799031809289, -122.3121186394012 37.64046006879865, -122.3331115240696 37.63452718491919, -122.3547349569902 37.63024841509044, -122.3767820624834 37.62766467784431, -122.39904199978 37.62680067847555, -122.4213019370766 37.62766467784431, -122.4433490425698 37.63024841509044, -122.4649724754904 37.63452718491919, -122.4859653601588 37.64046006879865, -122.5061267261711 37.64799031809289, -122.525263397989 37.6570458858488, -122.5431918174698 37.66754010266531, -122.5597397832247 37.67937249080907, -122.5747480911474 37.6924297095073, -122.5880720610203 37.7065866231559, -122.5995829347827 37.72170748303641, -122.6091691328522 37.73764721204931, -122.6167373558282 37.75425278095299, -122.6222135199815 37.77136466365747, -122.6255435161578 37.78881835827151, -122.6266937830899 37.80644595985081, -122.6256516876331 37.8240777701569, -122.6224257060943 37.84154392921929, -122.6170454026273 37.85867605311196, -122.6095612025884 37.87530886211608, -122.6000439607786 37.89128178335519, -122.5885843266221 37.90644051206436, -122.5752919105175 37.92063851589617, -122.5602942578208 37.93373846707945, -122.5437356391442 37.94561358783169, -122.5257756678485 37.95614889518292, -122.5065877577301 37.96524233229358, -122.4863574359164 37.97280577443459, -122.4652805278525 37.97876589903504, -122.4435612329404 37.98306491057806, -122.4214101108562 37.98566111262407, -122.39904199978 37.98652932084305, -122.3766738887038 37.98566111262407, -122.3545227666196 37.98306491057806, -122.3328034717075 37.97876589903504, -122.3117265636436 37.97280577443459, -122.2914962418299 37.96524233229358, -122.2723083317115 37.95614889518292, -122.2543483604158 37.94561358783169, -122.2377897417392 37.93373846707945, -122.2227920890425 37.92063851589617, -122.2094996729379 37.90644051206436, -122.1980400387814 37.8912817833552, -122.1885227969716 37.87530886211608, -122.1810385969327 37.85867605311196, -122.1756582934657 37.84154392921929, -122.1724323119269 37.8240777701569, -122.1713902164701 37.80644595985081, -122.1713902164701 37.80644595985081))'
# poly = ports.select("POLYGON10KM").take(1)[0][0]
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

def write_csv_to_parquet(spark, df_to_write):
	# Write Parquet to S3 
	target_bucket_dir = "s3a://ais-ship-pings-parquet/"
	target_file_name = 'allPings_with_visitIndex_portName_final.parquet'
	df_to_write.write.mode("append").format('parquet').partitionBy('PORT_NAME').option('compression', 'snappy').save(target_bucket_dir + target_file_name)

## Script
## Get data
source_bucket_dir_ports = "s3a://major-us-ports-csv/"
source_file_name_ports = "geoPorts_v2.parquet"
ports = sqlContext.read.parquet(source_bucket_dir_ports + source_file_name_ports) # Fastest by an order of mag

source_bucket_dir_pings = "s3a://ais-ship-pings-parquet/"
source_file_name_pings = "pings.parquet"
pings = sqlContext.read.parquet(source_bucket_dir_pings + source_file_name_pings) # Fastest by an order of mag
# pings.count() = 218,689,038  

# Invoke Function for geoHashing Ports:
# see benchmarking results for cell_size_degrees
cell_size_degrees = 0.4
# cell_size_degrees = 0.5
# cell_size_degrees = 0.6
ports = ports.withColumn("GRID_CELLS", polygon_to_gridCell_hashSet_udf(ports.POLYGON10KM, psql.lit(cell_size_degrees)))
ports = ports.withColumn("GRID_CELLS", psql.expr("substring(GRID_CELLS, 2, (length(GRID_CELLS)-1))"))

# Explode geoHash cells from list types to (one row per cell) to get complete set of port geoHash cells
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
pings = pings.withColumn("GRID_X", psql.floor(pings.LON.cast('Double')/cell_size_degrees))
pings = pings.withColumn("GRID_Y", psql.floor(pings.LAT.cast('Double')/cell_size_degrees))

	# Testing: Find Bay Area Hits
	# ports_geoHash.filter((psql.col("Y") > 36) & (psql.col("Y") < 39) & (psql.col("X") < -120) & (psql.col("X") > -123)).show()
	# pings.filter((psql.col("LAT") > 36) & (psql.col("LAT") < 39) & (psql.col("LON") < -120) & (psql.col("LON") > -123)).select("LON", "LAT", "GRID_X", "GRID_Y").take(20)

# pingsB = pings.filter(pings.Status == 'moored').limit(250) # for testing
# jpings.filter(jpings.PORT_NAME.isNotNull()).count() # for testing
pings = pings.join(psql.broadcast(ports_geoHash.select("PORT_NAME", "POLYGON10KM", "LON_CELL", "LAT_CELL")), on = (pings.GRID_X == ports_geoHash.LON_CELL) & (pings.GRID_Y == ports_geoHash.LAT_CELL), how = 'left_outer')
	# pings.count() # Benchmarking experiment
	# pings.select("PORT_NAME").distinct().show() # Testing 
	# pings.select("PORT_NAME").distinct().count() # Testing: Should be 150 if all ports were matched.
	# pings_port_list = pings.select("PORT_NAME").distinct().collect()
	# pings_port_listed = [row[0] for row in pings_port_list]
	# port_ports = ports.select("PORT_NAME").distinct().collect()
	# port_ports_listed = [row[0] for row in port_ports]
	# missing_ports = [x for x in port_ports_listed if x not in pings_port_listed]

# Now test those that matched
# pings_to_test = pings.PORT_NAME.isNotNull().count()
# pings_to_test = pings.select("PORT_NAME").where("PORT_NAME is not Null") # works
pings_to_test = pings.filter(pings.PORT_NAME.isNotNull()) # 78621860
# pings_outside = pings.filter(pings.PORT_NAME.isNull()) # 78621860

# pings_to_test.count() # Benchmarking experiment
# pings_outside.count() # Benchmarking experiment

# Perform final checks
pings_to_test = pings_to_test.withColumn('LON_LAT', lonLatString_to_geoPoint_udf(pings_to_test.LON, pings_to_test.LAT))
pings_final = pings_to_test.withColumn('inPortTrue', polygon_contains_point_udf(pings_to_test.POLYGON10KM, pings_to_test.LON_LAT))
pings_final = pings_final.filter(pings_final.inPortTrue == True).drop("POLYGON10KM")
### Cumulative Time
# see: https://stackoverflow.com/questions/45737199/pyspark-window-function-with-condition
# 1. orderBy("BaseDateTime")
# 2. groupBy("PORT_NAME", "VesselType", "VesselName", "MMSI")
# 3. withColumn('cumulative_visit_time', )
# For Testing
# 'Port Everglades, FL'
# 319642000
# pings_test = pings_final.filter((pings_final.PORT_NAME == 'Port Everglades, FL') & (pings_final.MMSI == '319642000')).limit(200).collect()
# pings_test_df = spark.createDataFrame(pings_test)

# Create visit_index based on assuming a break in time-stamps > 48 hours is a new visit.
	# Requires using window functions on partitions by port and mmsi (ship identifier)
window_for_timediff = Window.partitionBy("PORT_NAME", "MMSI").orderBy("BaseDateTime")

df_lag = pings_final.orderBy("BaseDateTime").\
withColumn('BaseDateTime_prev', lag(pings_final.BaseDateTime, 1).over(window_for_timediff)).\
select(*[psql.col(x) for x in pings_final.columns], \
(psql.unix_timestamp(pings_final.BaseDateTime) - psql.unix_timestamp(psql.col('BaseDateTime_prev'))).alias('dt'))\
# withColumn('visit_index', psql.row_number().over(window_for_timediff).cast('INT')).\
# withColumn('visit_index', psql.when((psql.col("dt") > 1728), 1).otherwise(psql.col('visit_index'))).\

df_lag = df_lag.withColumn("indicator", (df_lag.dt > (48*3600)).cast("int")) # flag new visit starts
df_lag = df_lag.fillna({'indicator' : 0})
df_lag = df_lag.withColumn("subgroup", psql.sum("indicator").over(window_for_timediff))
window_for_visit_index = Window.partitionBy("PORT_NAME", "MMSI", "subgroup").orderBy("BaseDateTime")
df_lag = df_lag.withColumn("visit_index", psql.first("BaseDateTime").over(window_for_visit_index))

write_csv_to_parquet(sc, df_lag)