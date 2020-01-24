#!/usr/bin/env python3
# PySpark Tutorial : https://realpython.com/pyspark-intro/
# Access EC2 and Start Spark Cluster
# SSH to EC2 and launch spark
ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com # Spark Master
sh /usr/local/spark/sbin/start-all.sh # re-run this after adding a new worker IP to the slaves file if scaling horizontally
# /usr/local/spark/sbin/stop-all.sh # stop spark cluster

# Start spark cluster manually
# ec2/spark-ec2 --key-pair=courseexample --identity-file=courseexample.pem launch spark-cluster-example
bash export AWS_SECRET_ACCESS_KEY=AaBbCcDdEeFGgHhIiJjKkLlMmNnOoPpQqRrSsTtU export AWS_ACCESS_KEY_ID=ABCDEFG1234567890123
./spark-ec2 --key-pair=awskey --identity-file=awskey.pem --region=us-west-1 --zone=us-west-1a launch my-spark-cluster 

# start pyspark with master node
pyspark --master spark://10.0.0.7:7077
http://ec2-44-229-205-147.us-west-2.compute.amazonaws.com:8080/ # only works if security group opens 8080 port to myIP
# ===========================================================================
# Python3 script
# geospatial modules
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from osgeo import gdal_array
from osgeo import gdalconst
# import other modules:
import pandas as pd
# import numpy as np
from functools import reduce
import pyspark #pyspark.sql , pyspark.ml
import boto3 
import botocore
import datetime
import os

import sys
sys.path.append('./src')
import  as sf
	# If running as spark-submit script.
# conf = SparkConf().setAppName("AIS_Extract").setMaster(master)
# sc = SparkContext(conf=conf)
	# If running as pyspark shell
# launch pyspark shell
# ./usr/local/spark/bin/pyspark

# 1. initiate boto3 resource & get folders with GDB databases
s3 = boto3.resource('s3')
# for bucket in s3.buckets.all(): # print bucket names
# 	print(bucket.name)
ais_bucket = s3.Bucket('ais-ship-data-raw') # define the bucket
files_in_bucket = list(ais_bucket.objects.all()) # list all objects in the bucket
# iterate and remove any portion of the key beyond the last '/'
paths_in_bucket = [os.path.dirname(files_in_bucket[x].key) for x in range(len(files_in_bucket))] # return just the file paths but not the files names
all_folders = set(paths_in_bucket) # unique parent 'directories'
gdb_folders = [i for i in list(all_folders) if 'gdb' in i] # subset to those with gdb files
gdb_folders_spark  = sc.parallelize(list(gdb_folders))
# Find CSV files
keys_in_bucket = [files_in_bucket[x].key for x in range(len(files_in_bucket))] # get all bucket object keys
csv_files = [i for i in keys_in_bucket if 'csv' in i] # find all csv files

# 2. iterate on the folders to run parse_gdb and write_parquet_to_s3
client = boto3.client('s3')
# Download all of the files 
for i in range(len(gdb_folders)):
	files_to_fetch_i = list(ais_bucket.objects.filter(Prefix = gdb_folders[i]))
	files_to_fetch_i = [files_to_fetch_i[x].key for x in range(len(files_to_fetch_i))] # get keys only
	for j in range(len(files_to_fetch)):
		# create directory for storing the files
		os.makedirs(gdb_folders[i], exist_ok=True)
		client.download_file('ais-ship-data-raw', \
			# gdb_folders[i] + '/' + str(os.path.basename(files_to_fetch[j])), \
			str(files_to_fetch[j]), \
			str(files_to_fetch[j]))
			# gdb_folders[i] + '/' + str(os.path.basename(files_to_fetch[j])))
	local_path = gdb_folders[i]
	# Submit spark job.
	pings, vessels = parse_gdb(local_path)
# 

# ==========================================================================================
# Notes and rejects:
client.download_fileobj('ais-ship-data-raw', gdb_folders[0], os.path.basename(files_to_fetch[0]))

# Use for csv 
# need exception for reading csv's because some folders are parent directories and won't actually return anything.
spark.read.csv()

# Convert from pandas df to spark df
# https://docs.databricks.com/spark/latest/spark-sql/spark-pandas.html
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Generate a pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))
# Create a Spark DataFrame from a pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)
# Convert the Spark DataFrame back to a pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()

# But see also: https://stackoverflow.com/questions/37513355/converting-pandas-dataframe-into-spark-dataframe-error/56895546
# For imposing schema to ensure pd to sparkdf works.