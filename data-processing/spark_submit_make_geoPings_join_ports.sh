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
scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/ship-soot/data-processing/make_geoPings_join_ports.py ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# scp -i "Connor-Dibble-IAM-keypair.pem" /Users/Connor/Documents/Graduate\ School/Dibble_Research/Github_repos/ship-soot/ingestion/spark_submit_ports_geoParquet.sh ubuntu@ec2-44-232-197-79.us-west-2.compute.amazonaws.com:/home/ubuntu/Scripts/
# From master machine, start cluster
# sh /usr/local/spark/sbin/start-all.sh # re-run this after adding a new worker IP to the slaves file if scaling horizontally
# /usr/local/spark/sbin/stop-all.sh # stop spark cluster
# submit job to spark cluster
ln -s /tmp /newvolume/tmp # soft link new volume.
/usr/local/spark/bin/spark-submit --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar, /home/ubuntu/Scripts/make_geoPings_join_ports.py &> /usr/local/spark/logs/spark_run_log_`date '+%Y_%m_%d__%H_%M_%S'`_terminal

# Pyspark Shell For Testing:
# From PyRasterFrames documentation:
   # pyspark \
   #  --master local[*] \
   #  # --py-files pyrasterframes_2.11-0.8.5-python.zip \
   #  --jars pyrasterframes_2.11-0.8.5.jar \
   #  --packages org.locationtech.rasterframes:rasterframes_2.11:0.8.5,org.locationtech.rasterframes:pyrasterframes_2.11:0.8.5,org.locationtech.rasterframes:rasterframes-datasource_2.11:0.8.5 \
   #  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \ # these configs improve serialization performance \
   #  --conf spark.kryo.registrator=org.locationtech.rasterframes.util.RFKryoRegistrator \
   #  --conf spark.kryoserializer.buffer.max=500m
# pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar

### Attempts to use 3rd party integrations:
# Includes PyRasterFrames dependences:
# pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/pyrasterframes_2.11-0.8.5.jar,/usr/local/spark/jars/sfcurve-zorder_2.11-0.2.0.jar \
# 	--packages harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:2.1.0,org.codehaus.jackson:jackson-core-asl:1.9.12

# rasterframes dependencies:
# org.locationtech.rasterframes:rasterframes_2.11:0.8.5,org.locationtech.rasterframes:rasterframes-datasource_2.11:0.8.5

# GeoMesa
# --conf/usr/local/spark/conf/spark-defaults-geoMesa.conf 
# Start
# /usr/local/hadoop/sbin/start-dfs.sh
# /usr/local/hadoop/sbin/start-yarn.sh
# /usr/local/hbase/bin/start-hbase.sh
# Stop
# /usr/local/hadoop/sbin/stop-dfs.sh
# /usr/local/hadoop/sbin/stop-yarn.sh
# /usr/local/hbase/bin/stop-hbase.sh


# pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/geomesa-feature-all_2.11-2.4.0.jar,/opt/geomesa/dist/spark/geomesa-hbase-spark-runtime_2.11-2.4.0.jar,/opt/geomesa/dist/spark/geomesa-hbase-spark_2.11-2.4.0.jar,/opt/geomesa/dist/hbase/geomesa-hbase-distributed-runtime_2.11-2.4.0.jar,/opt/geomesa/dist/converters/geomesa-tools_2.11-2.4.0-data.jar,GeoMesa_Config/jars/geomesa-gt-spark_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-spark-sql_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-feature-common_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-hbase-datastore_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-tools_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-filter_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-utils_2.11-2.4.0.jar,GeoMesa_Config/jars/geomesa-spark-core_2.11-2.4.0.jar,/usr/local/spark/jars/geomesa-spark-jts_2.11-2.4.0.jar,/usr/local/hbase/conf/hbase-site.xml --conf spark.executor.extraClassPath=~/GeoMesa_Config/geomesa-base-on-s3.json
# https://repo1.maven.org/maven2/org/locationtech/geomesa/geomesa-spark-jts_2.11/2.4.0/geomesa-spark-jts_2.11-2.4.0.jar
# ,/usr/local/spark/jars/geomesa-fs-spark_2.11-2.4.0.jar
# ,GeoMesa_Config/jars/geomesa-fs-storage-parquet_2.11-2.4.0.jar
# ,GeoMesa_Config/jars/geomesa-accumulo-datastore_2.11-2.4.0.jar
# /opt/geomesa/dist/spark/geomesa-hbase-spark-runtime_2.11-2.4.0.jar
# /opt/geomesa/dist/spark/geomesa-hbase-spark_2.11-2.4.0.jar
# /opt/geomesa/dist/hbase/geomesa-hbase-distributed-runtime_2.11-2.4.0.jar
# /opt/geomesa/dist/converters/geomesa-tools_2.11-2.4.0-data.jar

# import geomesa_pyspark as gs
# conf = geomesa_pyspark.configure(
#     jars=['/path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar'],
#     packages=['geomesa_pyspark','pytz'],
#     spark_home='/path/to/spark/').\
#     setAppName('MyTestApp')


# pyspark --master spark://10.0.0.7:7077 --jars /usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar,geomesa-feature-all_2.11-2.4.0.jar --conf
#    {
#      "Classification": "hbase-site",
#      "Properties": {
#        "hbase.rootdir": "s3://hbase-geomesa-backend/hbase-root"
#      }
#    },
#    {
#      "Classification": "hbase",
#      "Properties": {
#        "hbase.emr.storageMode": "s3"
#      }
#    }
# 	]'

# ~/GeoMesa_Config/geomesa-hbase-on-s3.json