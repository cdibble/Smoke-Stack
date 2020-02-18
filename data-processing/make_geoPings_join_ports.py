# make_geoPings_join_Ports.py
#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql.functions import pandas_udf, PandasUDFType, lag
from pyspark.sql.window import Window
# import pyarrow # for pandas_udf functionality
from pyspark.sql import Row
from pyspark.sql.types import *
# import boto3
import botocore
import datetime
import os
# import rasterio
from shapely.geometry import Point, Polygon
import shapely.geometry
from shapely import wkt
import shapely
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
def lonLatString_to_geoPoint(lon, lat):
	points = shapely.geometry.Point(float(lon), float(lat))#.wkt
	# points = [float(lon), float(lat)]#.wkt
	return points
# Register UDF
lonLatString_to_geoPoint_udf = psql.udf(lonLatString_to_geoPoint)

# poly = 'POLYGON ((-122.1713902164701 37.80644595985081, -122.1725404834023 37.78881835827151, -122.1758704795785 37.77136466365747, -122.1813466437318 37.75425278095299, -122.1889148667078 37.73764721204931, -122.1985010647773 37.72170748303641, -122.2100119385397 37.7065866231559, -122.2233359084126 37.6924297095073, -122.2383442163353 37.67937249080907, -122.2548921820902 37.66754010266531, -122.272820601571 37.6570458858488, -122.2919572733889 37.64799031809289, -122.3121186394012 37.64046006879865, -122.3331115240696 37.63452718491919, -122.3547349569902 37.63024841509044, -122.3767820624834 37.62766467784431, -122.39904199978 37.62680067847555, -122.4213019370766 37.62766467784431, -122.4433490425698 37.63024841509044, -122.4649724754904 37.63452718491919, -122.4859653601588 37.64046006879865, -122.5061267261711 37.64799031809289, -122.525263397989 37.6570458858488, -122.5431918174698 37.66754010266531, -122.5597397832247 37.67937249080907, -122.5747480911474 37.6924297095073, -122.5880720610203 37.7065866231559, -122.5995829347827 37.72170748303641, -122.6091691328522 37.73764721204931, -122.6167373558282 37.75425278095299, -122.6222135199815 37.77136466365747, -122.6255435161578 37.78881835827151, -122.6266937830899 37.80644595985081, -122.6256516876331 37.8240777701569, -122.6224257060943 37.84154392921929, -122.6170454026273 37.85867605311196, -122.6095612025884 37.87530886211608, -122.6000439607786 37.89128178335519, -122.5885843266221 37.90644051206436, -122.5752919105175 37.92063851589617, -122.5602942578208 37.93373846707945, -122.5437356391442 37.94561358783169, -122.5257756678485 37.95614889518292, -122.5065877577301 37.96524233229358, -122.4863574359164 37.97280577443459, -122.4652805278525 37.97876589903504, -122.4435612329404 37.98306491057806, -122.4214101108562 37.98566111262407, -122.39904199978 37.98652932084305, -122.3766738887038 37.98566111262407, -122.3545227666196 37.98306491057806, -122.3328034717075 37.97876589903504, -122.3117265636436 37.97280577443459, -122.2914962418299 37.96524233229358, -122.2723083317115 37.95614889518292, -122.2543483604158 37.94561358783169, -122.2377897417392 37.93373846707945, -122.2227920890425 37.92063851589617, -122.2094996729379 37.90644051206436, -122.1980400387814 37.8912817833552, -122.1885227969716 37.87530886211608, -122.1810385969327 37.85867605311196, -122.1756582934657 37.84154392921929, -122.1724323119269 37.8240777701569, -122.1713902164701 37.80644595985081, -122.1713902164701 37.80644595985081))'
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

def polygon_contains_point(polygonWKT, pointLon, pointLat):
   sh_polygon = shapely.wkt.loads(polygonWKT)
   sh_point = shapely.geometry.Point(float(pointLon), float(pointLat))
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
# Filter to VesselCategory != "Other"
vessel_types = [30, 21, 22, 21, 32, 52, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89]
pings = pings.where(pings.VesselType.cast("Double").isin(vessel_types))

# Invoke Function for geoHashing Ports:
# see benchmarking results for cell_size_degrees
cell_size_degrees = 0.4
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

# pingsD = pings.filter(pings.Status == 'moored').limit(250) # for testing
# jpings.filter(jpings.PORT_NAME.isNotNull()).count() # for testing
# pings = pings.join(psql.broadcast(ports_geoHash.select("PORT_NAME", "POLYGON10KM", "LON_CELL", "LAT_CELL")), on = (pings.GRID_X == ports_geoHash.LON_CELL) & (pings.GRID_Y == ports_geoHash.LAT_CELL), how = 'inner') # broadcast doesn't seem to improve performance
pings = pings.join(ports_geoHash.select("PORT_NAME", "POLYGON10KM", "LON_CELL", "LAT_CELL"), on = (pings.GRID_X == ports_geoHash.LON_CELL) & (pings.GRID_Y == ports_geoHash.LAT_CELL), how = 'inner')
###### Benchmarking experiment ######
# pings.count() 
# pings.select("PORT_NAME").distinct().show() # Testing 
# pings.select("PORT_NAME").distinct().count() # Testing: Should be 150 if all ports were matched.
# pings_port_list = pings.select("PORT_NAME").distinct().collect()
# pings_port_listed = [row[0] for row in pings_port_list]
# port_ports = ports.select("PORT_NAME").distinct().collect()
# port_ports_listed = [row[0] for row in port_ports]
# missing_ports = [x for x in port_ports_listed if x not in pings_port_listed]
####################################
# Drop uneeded columns
drop_cols = {"IMO", "CallSign", "GRID_X", "GRID_Y", "LON_CELL", "LAT_CELL"}
pings = pings.select([columns for columns in pings.columns if columns not in drop_cols])

### Write Postgres database: "pings_commercial" = pings dropping "VesselCatgeory" == "Other" 
# pings = pings.repartition(100)
pings = pings.repartition($"PORT_NAME")
saveMode="overwrite"
pings.write \
.format("jdbc") \
.option("driver", "org.postgresql.Driver") \
.option("dbtable", "pings_commercial") \
.option("url", 'jdbc:postgresql://10.0.0.14:5432/pings_2015_to_2017') \
.option("user", "db_user") \
.option("password", "look_at_data") \
.save(mode=saveMode)

# Apply UDF to check wehther geoHash match is truly in port 20 km polygon.
# pings_to_test = pings.withColumn('LON_LAT', lonLatString_to_geoPoint_udf(pings.LON, pings.LAT))
# pings_final = pings.withColumn('inPortTrue', polygon_contains_point_udf(pings.POLYGON10KM, pings.LON, pings.LAT))
# pings_final = pings_final.drop("POLYGON10KM")
# pings_final_in_port = pings_final.filter(pings_final.inPortTrue == True)

# OR skip the UDF and just drop the polygon
pings_final_in_port = pings.drop("POLYGON10KM")

### Write Postgres database: "pings_commercial" = pings dropping "VesselCatgeory" == "Other" 
# pings_final_in_port = pings_final_in_port.repartition($"PORT_NAME")
# saveMode="overwrite"
# pings_final_in_port.write \
# .format("jdbc") \
# .option("driver", "org.postgresql.Driver") \
# .option("dbtable", "pings_commercial_with_udf") \
# .option("url", 'jdbc:postgresql://10.0.0.14:5432/pings_2015_to_2017') \
# .option("user", "db_user") \
# .option("password", "look_at_data") \
# .save(mode=saveMode)

### Assigning Visits for each PORT_NAME and VesselName
# see: https://stackoverflow.com/questions/45737199/pyspark-window-function-with-condition
# For Testing
# 'Port Everglades, FL'
# 319642000
# pings_test = pings_final.filter((pings_final.PORT_NAME == 'Port Everglades, FL') & (pings_final.MMSI == '319642000')).limit(200).collect()
# pings_test_df = spark.createDataFrame(pings_test)

# Create visit_index based on assuming a break in time-stamps > 48 hours is a new visit.
	# Requires using window functions on partitions by port and ship name
# First, define Window
window_for_timediff = Window.partitionBy("PORT_NAME", "VesselName").orderBy("BaseDateTime")
# Then create column with time lagged by one ping for each ship and port
# And create a column, 'dt', that representes the time difference between pings on the same data partitions
df_lag = pings_final_in_port. \
withColumn('BaseDateTime_prev', lag(pings.BaseDateTime, 1).over(window_for_timediff)).\
select(*[psql.col(x) for x in pings_final_in_port.columns], \
(psql.unix_timestamp(pings_final_in_port.BaseDateTime) - psql.unix_timestamp(psql.col('BaseDateTime_prev'))).alias('dt'))\

# Flag the 'dt' values with a 1 if the time between pings is > 48 hours
df_lag = df_lag.withColumn("indicator", (df_lag.dt > (48*3600)).cast("int")) # flag new visit starts
df_lag = df_lag.fillna({'indicator' : 0}) # Fill NA values with 0
# Apply rolling cumulative sum over the data partitions to build index of visits for each ship and port.
df_lag = df_lag.withColumn("subgroup", psql.sum("indicator").over(window_for_timediff))

# write_csv_to_parquet(sc, df_lag)

### Write Postgres database: "pings_commercial" = pings dropping "VesselCatgeory" == "Other" 
# pings = pings.repartition(200)
df_lag = df_lag.repartition($"PORT_NAME")
saveMode="overwrite"
df_lag.write \
.format("jdbc") \
.option("driver", "org.postgresql.Driver") \
.option("dbtable", "pings_final_in_port_with_visitSubgroup_noudf") \
.option("url", 'jdbc:postgresql://10.0.0.14:5432/pings_2015_to_2017') \
.option("user", "db_user") \
.option("password", "look_at_data") \
.save(mode=saveMode)

