#!/usr/bin/env python3
# Imports:
from osgeo import gdal
from osgeo import ogr
# from osgeo import osr
# from osgeo import gdal_array
# from osgeo import gdalconst
import pandas as pd
import datetime
from pyspark.sql import functions as psql
gdal.UseExceptions() # raise exceptions instead of returning errors


# import com.esri.gdb._
# import com.esri.udt._
gdb_path = "/Users/Connor/Documents/InisightDE/1_Project/Data/01_January_2009/Zone1_2009_01.gdb"
gdb_path = 's3a://ais-ship-data-raw/Data/2010/09_September_2010/Zone4_2010_09/Zone4_2010_09.gdb'
import os
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
sc._jsc.hadoopConfiguration().set("fs.s3a.region", os.environ["AWS_REGION"])

# TODO: 
# Convert pandas manipulations to sparkDF
# add zone information? comes from file structure...
# drop unneeded columns consider dropping data fields: 'type' , 'reciever type', 'reciever id'
# convert the panda df output to a spark rdd
# write to s3 in parquet format with sensible partition keys
# parse file structure and iterate over files

# Build function for importing attributes, clipping files.
	# TODO: Don't iterate over unwanted layers.
def parse_gdb_local(gdb_path, out_path=None):
	import os
	import boto3
	client = boto3.client('s3')
	os.makedirs(gdb_path, exist_ok=True) # create directory strcture for download.
	files_to_fetch_i = list(ais_bucket.objects.filter(Prefix = gdb_path)) # get folder_i
	files_to_fetch_i = [files_to_fetch_i[x].key for x in range(len(files_to_fetch_i))] # get keys only
	client.download_file('ais-ship-data-raw', \
			# gdb_folders[i] + '/' + str(os.path.basename(files_to_fetch[j])), \
			str(files_to_fetch[j]), \
			str(files_to_fetch[j]) )

	driver = ogr.GetDriverByName("OpenFileGDB") # get appropriate driver.
	ds = driver.Open(gdb_path, 0) # open GDB file; 0 is for read-only, 1 is for writeable.
	numLayers = ds.GetLayerCount() # Find number of layers in file.
	verbose = False
	for i in range(numLayers): # iterate over layers
		current_layer = ds.GetLayer(i) # load a layer
		current_layer.ResetReading() # reset from prior reads
		current_layer_field_count = current_layer.GetLayerDefn().GetFieldCount() # print the number of field in the layer
		features_in_layer = current_layer.GetFeatureCount()
		layer_name = current_layer.GetLayerDefn().GetName()
		if verbose == True:
			print("\nLayer Name: %s" %(layer_name))
			print("No. of features in this layer: %d" %(features_in_layer)) # print the number of features in layer
			print("Field Names in this Layer: ")
			for j in range(current_layer_field_count):
				current_layer.GetLayerDefn().GetFieldDefn(j).GetName() # print the names of the fields
		if layer_name in ['Broadcast']:
			for feature in range(1, features_in_layer + 1): # for each feature, get geometry.
				try:
					feature_contents = current_layer.GetFeature(feature).ExportToJson()
					# USE SPARK STARTING HERE:::
					feature_df = pd.read_json(feature_contents).T
					feature_df.loc['properties']['coordinates'] = feature_df.loc['geometry']['coordinates']
					if feature == 1:
						feature_list = feature_df.drop(['geometry', 'type', 'id'])#[list(feature_df.columns)]
						feature_list['id'] = current_layer.GetFeature(feature).GetFID()
						feature_list = feature_list.set_index('id')
					if feature > 1:
						feature_list_next_row = feature_df.drop(['geometry', 'type', 'id'])#[list(feature_df.columns)]
						feature_list_next_row['id'] = current_layer.GetFeature(feature).GetFID()
						feature_list_next_row = feature_list_next_row.set_index('id')
						feature_list = feature_list.append(feature_list_next_row)
				except:
					print("ERROR: No Geometry For This Broadcast Feature")
			try:
				feature_list['BaseDateTime'] = pd.to_datetime(feature_list['BaseDateTime'], format = "%Y/%m/%d %H:%M:%S") # strftime goes back to string
				feature_list['Year'] = feature_list['BaseDateTime'].dt.strftime("%Y") # strftime goes back to string
				feature_list['Month'] = feature_list['BaseDateTime'].dt.strftime("%m") # strftime goes back to string
			except:
				print("ERROR: Date parsing not working")
		if layer_name in ['Vessel']:
			for vessel_feature in range(1, features_in_layer + 1): # for each feature, get geometry.
				try:
					vessel_feature_contents = current_layer.GetFeature(vessel_feature).ExportToJson()
					vessel_feature_df = pd.read_json(vessel_feature_contents).T
					if vessel_feature == 1:
						vessel_feature_list = vessel_feature_df.drop(['geometry', 'type', 'id'])#[list(vessel_feature_df.columns)]
						vessel_feature_list['id'] = current_layer.GetFeature(vessel_feature).GetFID()
						vessel_feature_list = vessel_feature_list.set_index('id')
					if vessel_feature > 1:
						vessel_feature_list_next_row = vessel_feature_df.drop(['geometry', 'type', 'id'])#[list(vessel_feature_df.columns)]
						vessel_feature_list_next_row['id'] = current_layer.GetFeature(vessel_feature).GetFID()
						vessel_feature_list_next_row = vessel_feature_list_next_row.set_index('id')
						vessel_feature_list = vessel_feature_list.append(vessel_feature_list_next_row)
				except:
					print("ERROR: No Geometry For This Vessel Feature")
	return feature_list, vessel_feature_list


## Version 2: Using FileGDB (https://github.com/mraad/FileGDB)
# TODO, add verbose option to print schema and tables
def parse_FileGDB(gdb_path, out_path=None):
	jvm = spark._jvm # get ref to underlying JVM.
	gdb = jvm.com.esri.gdb.FileGDB # get ref to FileGDB Scala object
	# gdb.listTables(gdb_path) # List all tables in gdb.
	# schema_Broadcast = gdb.schema(gdb_path,'Broadcast') # get schema for 'Broadcast'
	# schema_Vessel = gdb.schema(gdb_path,'Vessel') # get schema for 'Vessel'
	## Create Spark DF from GDB
	# Needs credentials
	## Ping Table
	ping_table = spark.read \
		.format("com.esri.gdb") \
		.options(path="s3a://ais-ship-data-raw/" + gdb_path, name="Broadcast") \
		.load()
	# ping_table.registerTempTable("Broadcast") # register table for upcoming SQL ops; not sure what this means.
	# Use sql (pyspark.sql, i think) commands to map pings to evenly spaced cells, then get the lat/lon of the cells
		# Note - the cells are based on the lat/long ranges in this shard only, so are not meaningful overall
		# cell_1 = 0.001
		# cell_2 = cell_1 * 0.5
		# sql(f"select cast(floor(Shape.x/{cell_1}) as INT) as q,cast(floor(Shape.y/{cell_1}) as INT) as r from Broadcast")\
		# 	.registerTempTable("QR")
		# sql(f"select q*{cell_1}+{cell_2} as x,r*{cell_1}+{cell_2} as y,count(1) as pop from QR group by q,r having pop > 100").show()
	# make datetime objects, extract Year and Month.
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('BaseDateTime')
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('Year') # need to extract %Y and convert to string 
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('Month') # need to extract %m and convert to string 
	## Vessel Table
	vessel_table = spark.read \
		.format("com.esri.gdb") \
		.options(path=gdb_path, name="Vessel") \
		.load()
	# vessel_table.registerTempTable("Vessel") # register table for upcoming SQL ops; not sure what this means.
	return ping_table, vessel_table

# Test parse_gdb()
# feature_list, vessel_feature_list = parse_gdb(gdb_path)

# manipulating timestamps with pyspark.sql
# https://stackoverflow.com/questions/57642177/extracting-the-year-from-date-in-pyspark-dataframe

def parse_csv(csv_file, out_path=None):
	from pyspark.sql import functions as psql	# Not sure what these files actually look like (Broadcast vs. Vessel tables).... Need to look first.
	# sqlContext = SQLContext(sc)
	csv_i = spark.read.format("csv").option("inferSchema", \
		True).option("header", True).load("s3a://ais-ship-data-raw/" + csv_file)
	csv_i = csv_i.withColumn("Year", psql.year('BaseDateTime'))
	csv_i = csv_i.withColumn("Month", psql.month('BaseDateTime'))
	return(csv_i)

	
# Test
parse_csv(csv_files[0])
sc.textFile("s3://ais-ship-data-raw" + csv_files[0]).take(1)
sc.readcsv("s3://ais-ship-data-raw" + csv_files[0]).take(1)





#####################################################
	# Create Spark DF from GDB
	## Ping Table
	ping_table = spark.read \
		.format("com.esri.gdb") \
		.options(path=gdb_path, name="Broadcast") \
		.load()
	# ping_table.registerTempTable("Broadcast") # register table for upcoming SQL ops; not sure what this means.
	# Use sql (pyspark.sql, i think) commands to map pings to evenly spaced cells, then get the lat/lon of the cells
		# Note - the cells are based on the lat/long ranges in this shard only, so are not meaningful overall
		# cell_1 = 0.001
		# cell_2 = cell_1 * 0.5
		# sql(f"select cast(floor(Shape.x/{cell_1}) as INT) as q,cast(floor(Shape.y/{cell_1}) as INT) as r from Broadcast")\
		# 	.registerTempTable("QR")
		# sql(f"select q*{cell_1}+{cell_2} as x,r*{cell_1}+{cell_2} as y,count(1) as pop from QR group by q,r having pop > 100").show()
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('BaseDateTime')
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('Year') # need to extract %Y and convert to string 
	ping_table.select(to_timestamp('BaseDateTime', '%Y/%m/%d %H:%M:%S')).alias('Month') # need to extract %m and convert to string 
	## Vessel Table
	vessel_table = spark.read \
		.format("com.esri.gdb") \
		.options(path=gdb_path, name="Vessel") \
		.load()
	# vessel_table.registerTempTable("Vessel") # register table for upcoming SQL ops; not sure what this means.
	return ping_table, vessel_table

# pandas: read_json, to_parquet
def write_pings_parquet_to_s3(object_to_write = None, bucket_dir = "s3a://ais-ship-pings-parquet/", file_name = None):
	spark_object_to_write = spark.createDataFrame(object_to_write) # converts pandas df to spark RDD with @F.pandas_udf('data_class', F.PandasUDFType.SCALAR)
	spark_object_to_write.write.mode("append").format('parquet').partitionBy('Year', 'Month').option('compression', 'snappy').save(bucket_dir + file_name)

def write_vessels_parquet_to_s3(object_to_write = None, bucket_dir = "s3a://ais-ship-vessel-info-parquet/", file_name = None):
	spark_object_to_write = spark.createDataFrame(object_to_write) # converts pandas df to spark RDD with @F.pandas_udf('data_class', F.PandasUDFType.SCALAR)
	spark_object_to_write.write.mode("append").format('parquet').option('compression', 'snappy').save(bucket_dir + file_name)

def write_csv_parquet_to_s3(object_to_write = None, bucket_dir = "s3a://ais-ship-pings-parquet/", file_name = None):
	# spark_object_to_write = spark.createDataFrame(object_to_write) # converts pandas df to spark RDD with @F.pandas_udf('data_class', F.PandasUDFType.SCALAR)
	object_to_write.write.mode("append").format('parquet').option('compression', 'snappy').save(bucket_dir + file_name)

# Test write_parquet_to_s3
bucket_dir = "s3://ais-ship-pings-parquet/Data/"  # S3 Path need to mention
file_name = "ais-pings.parquet"
write_parquet_to_s3(feature_list, bucket_dir, file_name)

# ========================================================================
# Get contents of AttributeUnits layer once to check it out. current_layer defined for i = 4
# [current_layer.GetFeature(x).ExportToJson() for x in range(1,features_in_layer+1)]
# OUTPUT: 
	# ['{"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "SOG", "Unit": "Knots"}, "id": 1}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "COG", "Unit": "Degrees"}, "id": 2}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "Heading", "Unit": "Degrees"}, "id": 3}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "ROT", "Unit": "Degrees/Minute"}, "id": 4}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "Length", "Unit": "Meters"}, "id": 5}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "Width", "Unit": "Meters"}, "id": 6}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "DimensionComponents", "Unit": "Meters"}, "id": 7}', '
	# {"type": "Feature", "geometry": null, "properties": 
	# {"Attribute": "Draught", "Unit": "1/10 Meters"}, "id": 8}']
# ========================================================================
# DEV:
# get_object and download_file methods for s3 objects
# https://stackoverflow.com/questions/37442444/download-s3-files-with-boto
client = boto3.client('s3')

# if your bucket name is mybucket and the file path is test/abc.txt
# then the Bucket='mybucket' Prefix='test'

resp = client.list_objects_v2(Bucket="<your bucket name>", Prefix="<prefix of the s3 folder>") 

for obj in resp['Contents']:
	key = obj['Key']
	#to read s3 file contents as String
	response = client.get_object(Bucket="<your bucket name>",
						 Key=key)
	print(response['Body'].read().decode('utf-8'))




##### For Testing FileGDB package from github
jvm = spark._jvm # get ref to underlying JVM.
gdb = jvm.com.esri.gdb.FileGDB # get ref to FileGDB Scala object
gdb_path = 'Data/2011/05/Zone14_2011_05.gdb/Zone14_2011_05.gdb' # for testing
gdb_path = "s3a://ais-ship-data-raw/" + gdb_path
ping_table = spark.read \
		.format("com.esri.gdb") \
		.options(path=gdb_path, name="Broadcast") \
		.load()

ping_table = sc.textFile('s3a://ais-ship-data-raw/' + gdb_path) \
		.format("com.esri.gdb") \
		.options(path="s3a://ais-ship-data-raw/" + gdb_path, name="Broadcast") \
		.load()			