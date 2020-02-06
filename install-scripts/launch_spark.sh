#!/bin/bash
# LAUNCH SPARK
# PySpark Tutorial : https://realpython.com/pyspark-intro/
# Access EC2 and Start Spark Cluster
# SSH to EC2 and launch spark
# ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com # Spark Master
ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com
sh /usr/local/spark/sbin/start-all.sh # re-run this after adding a new worker IP to the slaves file if scaling horizontally
# /usr/local/spark/sbin/stop-all.sh # stop spark cluster

## Send /usr/local/spark/conf/spark-defaults.conf to worker nodes
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.4:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.6:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.11:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.9:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.5:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.10:/usr/local/spark/conf/spark-env.sh
scp ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/usr/local/spark/conf/spark-env.sh 10.0.0.12:/usr/local/spark/conf/spark-env.sh

ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-35-163-99-143.us-west-2.compute.amazonaws.com
## RUN SPARK SUBMIT
# push script to master EC2
scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/ship-soot/ingestion/pySpark_DB_Extract.py ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# submit job to spark cluster
/usr/local/spark/bin/spark-submit --master spark://10.0.0.7:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar, /home/ubuntu/Scripts/pySpark_DB_Extract.py &> /usr/local/spark/logs/spark_run_log_`date '+%Y_%m_%d__%H_%M_%S'`_terminal
	# --executor-memory 6g --total-executor-cores 7
# SPARK GUI:  only works if security group opens 8080 port to myIP
# http://ec2-44-229-205-147.us-west-2.compute.amazonaws.com:8080/
http://ec2-44-232-197-79.us-west-2.compute.amazonaws.com:8080/
# Spark Log Level
sc.setLogLevel("ERROR") # Stop stream of warnings
## DEV / TEST
# start pyspark with master node
pyspark --master local[*]

pyspark\
  --master spark://10.0.0.7:7077 \
  --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar #, /home/ubuntu/Scripts/pySpark_DB_Extract.py
  # --jars /usr/local/spark/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar\
  # --packages org.apache.hadoop:hadoop-client:2.6.5
  #--conf spark.ui.enabled=true\
  #--jars "aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar" \
  #--packages ${PACKAGES} \
  #--num-executors 4\
  #--driver-memory 30G\
  #--executor-memory 30G\
  # --conf spark.ui.enabled=false\
  # --exclude-packages org.scala-lang:scala-reflect



## PACKAGE / DEPENDENCY HANDLING (FOR FileGDB)
# export PACKAGES="com.esri:filegdb:0.37"
# "aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar" # add as -jars option
# com.esri:webmercator_$2.12:1.4,com.esri:filegdb_$2.12:$0.18
# com.esri:spark-gdb:0.7; com.esri:filegdb:0.12.5"
  # org.apache.hadoop:hadoop-common:jar:2.6.5
  # com.esri:webmercator_2.12:1.4,com.esri:filegdb_2.12:0.18,com.esri:filegdb:0.12.5
# pyspark\
# 	--master spark://10.0.0.7:7077 \
# 	--packages com.amazonaws:aws-java-sdk:1.7.4;org.apache.hadoop:hadoop-aws:2.6.5;org.apache.hadoop:hadoop-client:jar:2.6.5;\
# 	com.esri:webmercator_2.12:1.4;com.esri:filegdb_2.12:0.18;com.esri:filegdb:0.12.5\

# packages that are supposedly needed?
	# org.apache.hadoop:hadoop-aws:2.6.5 # It's possible that hadoop-aws:2.7.1 could work? (answer, didn't help) https://stackoverflow.com/questions/36165704/pyspark-using-iam-roles-to-access-s3
	# org.apache.hadoop:hadoop-client:2.6.5,
	# 'org.apache.hadoop:hadoop-common:jar:2.6.5'
# Issues driven by conflict in ~/.m2/repository and /home/ubuntu/.ivy2

# jars that are supposedly needed
  # --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-common:2.6.5,org.apache.hadoop:hadoop-aws:2.6.5,org.apache.hadoop:hadoop-client:2.6.5 \
  # --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1 \
 # Jars that worked with PySpark and FileGDB
	# --jars /usr/local/spark/jars/spark-gdb-0.7.jar,/usr/local/spark/jars/filegdb-0.37.jar,/usr/local/spark/jars/filegdb-0.37-2.11.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar\
# Spark: 2.4.4
# Hadoop: 2.7.3
# Haddop-AWS: hadoop-aws-2.7.3.jar
# AWS-JAVA-SDK: aws-java-sdk-1.7.3.jar
# Scala: 2.11
  #--master local[*] \


# Debug: Bucket Permissions
# aws2 s3api list-objects --bucket "ais-ship-data-raw" --prefix exampleprefix 	
########################
# sc.stop() # Stop spark context
# Start new spark context with custom files
# sc = SparkContext(master, app_name, pyFiles=['/path/to/scripts_to_send_to_workers.py'])
# spark.hadoop.fs.s3a.impl         org.apache.hadoop.fs.s3a.S3AFileSystem


# Start spark cluster manually
# ec2/spark-ec2 --key-pair=courseexample --identity-file=courseexample.pem launch spark-cluster-example
# bash export AWS_SECRET_ACCESS_KEY=<access-key> export AWS_ACCESS_KEY_ID=<key-id>
# need to get app-name out of this to supply to spark-submit --class <...>
# ./spark-ec2 --key-pair=awskey --identity-file=awskey.pem --region=us-west-2 --zone=us-west-2c launch my-spark-cluster