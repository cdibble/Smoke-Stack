#!/usr/bin/env python3
# Imports
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as psql
from pyspark.sql.types import *
import boto3 
import botocore
import datetime
import os
import timeit
import time
	# If running as spark-submit script.
conf = SparkConf().setAppName("AIS_Extract") #.setMaster(spark://10.0.0.7:7077)
# sc = SparkContext(conf=conf, pyFiles = ["/home/ubuntu/Scripts/extract_gdb_functions.py"] ) # to pass scprits to spark context
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
# spark = SparkSession.builder.appName("AIS_Extract") # https://github.com/carolsong/tonebnb/blob/master/batching/util.py

###### Functions to Parse Files
def parse_csv(spark):
	""" Function : Reads CSV files from a source S3 bucket and writes to a target S3 bucket in Parquet
		:type spark: A spark session.
	"""
	source_bucket_dir = "s3a://ais-ship-data-raw/"
	source_file_name = "*/*/*/*/*/*.csv"
	# source_file_name = csv_files[0] # For Testing
	# TODO: list matched files for QA
	schema = StructType([
	StructField('MMSI',StringType(),True),
	StructField('BaseDateTime',StringType(),True),
	StructField('LAT',StringType(),True),
	StructField('LON',StringType(),True),
	StructField('SOG',StringType(),True),
	StructField('COG',StringType(),True),
	StructField('Heading',StringType(),True),
	StructField('VesselName',StringType(),True),
	StructField('IMO',StringType(),True),
	StructField('CallSign',StringType(),True),
	StructField('VesselType',StringType(),True),
	StructField('Status',StringType(),True),
	StructField('Length',StringType(),True),
	StructField('Width',StringType(),True),
	StructField('Draft',StringType(),True),
	StructField('Cargo',StringType(),True)
	])
	try:
		# Read CSVs, add partition columns
		csv_i = sqlContext.read.csv(source_bucket_dir + source_file_name, schema = schema, header = True) # Fastest by an order of mag
		csv_i = csv_i.withColumn("BaseDateTime", psql.to_timestamp('BaseDateTime'))
		csv_i = csv_i.withColumn("Year", psql.year('BaseDateTime'))
		csv_i = csv_i.withColumn("Month", psql.month('BaseDateTime'))
	except:
		print("ERROR")
	return(csv_i)

def write_csv_to_parquet(spark, csv_i):
		# Write Parquet to S3 
		target_bucket_dir = "s3a://ais-ship-pings-parquet/"
		target_file_name = 'pings.parquet'
		csv_i.write.mode("append").format('parquet').partitionBy('Year', 'Month').option('compression', 'snappy').save(target_bucket_dir + target_file_name)

# Script to get files/folders for parsing and call functions
## 1. initiate boto3 resource & get folders with GDB databases
s3 = boto3.resource('s3') # TODO: Initialize is expensive. Can I do all of the client. operations below with this resrouce?
ais_bucket = s3.Bucket('ais-ship-data-raw') # define the bucket
files_in_bucket = list(ais_bucket.objects.all()) # list all objects in the bucket
# Find .gdb folders: iterate and remove any portion of the key beyond the last '/'
paths_in_bucket = [os.path.dirname(files_in_bucket[x].key) for x in range(len(files_in_bucket))] # return just the file paths but not the files names
all_folders = set(paths_in_bucket) # unique parent 'directories'
gdb_folders = [i for i in list(all_folders) if 'gdb' in i] # subset to those with gdb files
# gdb_folders = ['s3://ais-ship-data-raw/' + i for i in gdb_folders]
gdb_folders_spark  = sc.parallelize(list(gdb_folders[0:2])) # create spark RDD from list
# Find CSV files:
keys_in_bucket = [files_in_bucket[x].key for x in range(len(files_in_bucket))] # get all bucket object keys
csv_files = [i for i in keys_in_bucket if 'csv' in i] # find all csv files
# csv_files_spark = sc.parallelize(csv_files[0:10]) # create spark RDD from list

# 2. Parse CSVs
csv_i = parse_csv(sc)

write_csv_to_parquet(sc, csv_i)

# 3. TODO Parse GDBs
# gdb_tables = gdb_folders_spark.map(lambda x: parse_gdb(x, bucket_in = 'ais-ship-data-raw'))
# print(gdb_tables)
# print(gdb_tables.first())
# pings, vessels = parse_gdb(gdb_folders[0], bucket_in = 'ais-ship-data-raw')