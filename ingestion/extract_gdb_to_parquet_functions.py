#!/usr/bin/env python3
# Install dependencies:
#1. install gdal :: https://gis.stackexchange.com/questions/28966/python-gdal-package-missing-header-file-when-installing-via-pip/74060#74060?newreg=ff051bdd59f8443d954e7ef5f5f4a5e7
sudo apt-get install gdal-bin
sudo apt-get install libgdal-dev
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
pip3 install GDAL==2.2.3 # get version using gdalinfo --version

# Imports:
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from osgeo import gdal_array
from osgeo import gdalconst
import pandas as pd
import datetime
gdal.UseExceptions() # raise exceptions instead of returning errors
gdb_path = "/Users/Connor/Documents/InisightDE/1_Project/Data/01_January_2009/Zone1_2009_01.gdb"

# TODO: 
# add zone information? comes from file structure...
# drop unneeded columns consider dropping data fields: 'type' , 'reciever type', 'reciever id'
# convert the panda df output to a spark rdd
# write to s3 in parquet format with sensible partition keys
# parse file structure and iterate over files

# Build function for importing attributes, clipping files.
def parse_gdb(gdb_path, out_path=None):
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

# Test parse_gdb()
# feature_list, vessel_feature_list = parse_gdb(gdb_path)

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

# pandas: read_json, to_parquet
def write_parquet_to_s3(object_to_write, bucket_dir, file_name):
	spark_object_to_write = spark.createDataFrame(object_to_write)
	spark_object_to_write.write.mode("append").format('parquet').option('compression', 'snappy').save(bucket_dir + file_name)

# Tetst write_parquet_to_s3
bucket_dir = "s3://ais-ship-pings-parquet/Data/"  # S3 Path need to mention
file_name = "ais-pings.parquet"
write_parquet_to_s3(feature_list, bucket_dir, file_name)