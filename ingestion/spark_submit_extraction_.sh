#!/bin/bash
# spark_submit_extraction_.sh
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
# scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/ship-soot/ingestion/pySpark_DB_Extract.py ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/ship-soot/ingestion/spark_submit_extraction_.sh ubuntu@ec2-44-229-205-147.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# From master machine, start cluster
# sh /usr/local/spark/sbin/start-all.sh # re-run this after adding a new worker IP to the slaves file if scaling horizontally

# submit job to spark cluster
/usr/local/spark/bin/spark-submit --master spark://10.0.0.7:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar, /home/ubuntu/Scripts/pySpark_DB_Extract.py &> /usr/local/spark/logs/spark_run_log_`date '+%Y_%m_%d__%H_%M_%S'`_terminal
