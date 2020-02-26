#!/bin/bash
# spark_submit_ports_geoParuqet.sh
# see https://spark.apache.org/docs/latest/submitting-applications.html
# use deploy-mode = 'client' mode for deploys from EC2 master
# pass python script *.py where <application-jar> is; if many, pass as .zip file
# Template
# ./bin/spark-submit \
#   --class <main-class> \
#   --master <master-url> \
#   --deploy-mode <deploy-mode> \ 
#   --conf <key>=<value> \
#   ... # other options
#   <application-jar> \
#   [application-arguments]
###
# PREREQUISITE: From local machine, push code to Master
# ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com # Spark Master
ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com
scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/Smoke-Stack/data-processing/make_geoPings_join_ports.py ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/Smoke-Stack/ingestion/spark_submit_ports_geoParquet.sh ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/

# From master machine, start cluster
# sh /usr/local/spark/sbin/start-all.sh # re-run this after adding a new worker IP to the slaves file if scaling horizontally
# /usr/local/spark/sbin/stop-all.sh # stop spark cluster

# submit job to spark cluster
/usr/local/spark/bin/spark-submit --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar, /home/ubuntu/Scripts/make_geoPings_join_ports.py #&> /usr/local/spark/logs/spark_run_log_`date '+%Y_%m_%d__%H_%M_%S'`_terminal

# for local testing:
# pyspark --master local[*] --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar

# to run:
# pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar

# includes postgres jdbc jars
pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/postgresql-42.2.9.jar,/usr/local/spark/jars/postgresql-9.1-901-1.jdbc4.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar  \
