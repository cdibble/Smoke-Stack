# parquet_to_postgres.py

## COMMAND LINE
# create postgres db
sudo -u postgres -i
# createdb pings_db -O postgres # use different tablespace
psql -l  | grep pings_db
psql 
CREATE USER db_user WITH PASSWORD 'look_at_data';
CREATE DATABASE pings_2015_to_2017_v2 TABLESPACE pings_database;
GRANT ALL PRIVILEGES ON DATABASE pings_2015_to_2017_v2 TO db_user;

# Start postgres server
sudo service postgresql start
sudo service postgresql stop

# ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-35-160-239-28.us-west-2.compute.amazonaws.com
pyspark --master local[*] --jars /usr/local/spark/jars/postgresql-42.2.9.jar,/usr/local/spark/jars/postgresql-9.1-901-1.jdbc4.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar  \
pyspark --master spark://10.0.0.14:7077 --jars /usr/local/spark/jars/postgresql-42.2.9.jar,/usr/local/spark/jars/postgresql-9.1-901-1.jdbc4.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/hadoop-aws-2.7.1.jar  \
	
# --config spark.local.dir=/database/raw_data job.local.dir=/database/raw_data/ 
### PYTHON3
import boto3
conf = SparkConf().setAppName("Migrate_to_PostgreSQL_Database") #.setMaster(spark://10.0.0.7:7077)
from pyspark.sql import functions as psql
# PINGS
source_bucket_dir_pings = "s3a://ais-ship-pings-parquet/"
# source_file_name_pings = "pings_with_visitIndex_portName.parquet" # one month of data (Jan. 2017)
source_file_name_pings = "allPings_with_visitIndex_portName.parquet" # two years of data (2016-2017)
pings = sqlContext.read.parquet(source_bucket_dir_pings + source_file_name_pings) # Fastest by an order of mag
# get rid of columns not needed for Postgres
drop_cols = {"IMO", "CallSign", "GRID_X", "GRID_Y", "LON_CELL", "LAT_CELL", "LON_LAT", "inPortTrue", "indicator"}
pings = pings.select([columns for columns in pings.columns if columns not in drop_cols])

# pings = pings.withColumn("Year", psql.year('BaseDateTime'))
# pings = pings.filter(pings.Year == "2015")

# Read PORTS
source_bucket_dir_ports = "s3a://major-us-ports-csv/"
source_file_name_ports = "geoPorts_v2.parquet"
ports = sqlContext.read.parquet(source_bucket_dir_ports + source_file_name_ports) # Fastest by an order of mag

# compute cumulative time per visit?

#### Send to Postgres Database
####### TEMPLATE FROM SARAH ####
#   table0 = spark.read \
#         .format("parqet") \
#         #.option("driver", "org.postgresql.Driver") \
#         #.option("url", 'jdbc:postgresql://10.0.0.10:5432/root') \
#         .option("dbtable", query) \
#         .option("user", "root") \
#         .option("password", "RWwuvdj75Me4") \
#         .option("partitionColumn", "rno") \ # 'rno' parition column
#         .option("lowerBound", 0).option("upperBound", rowNum) \ # (upperBound - lowerBound )/ numPartitions = # rows per partition
#         .option("numPartitions", numPartitions) \
#         .load()  \
#         .cache() # or persist()
# # for partitioning on read:
# .option("partitionColumn", "BaseDateTime") \
# .option("lowerBound", 0).option("upperBound", rowNum) \
# .option("numPartitions", numPartitions) \
####################################
# rowNum = pings.count()
# Write PINGS
pings = pings.repartition(100)
saveMode="append"
pings.write \
.format("jdbc") \
.option("driver", "org.postgresql.Driver") \
.option("dbtable", "pings_db") \
.option("url", 'jdbc:postgresql://10.0.0.14:5432/pings_2015_to_2017') \
.option("user", "db_user") \
.option("password", "look_at_data") \
.save(mode=saveMode)


# Write PORTS
ports = ports.repartition(10)
saveMode="append"
ports.write \
.format("jdbc") \
.option("driver", "org.postgresql.Driver") \
.option("dbtable", "ports_db") \
.option("url", 'jdbc:postgresql://10.0.0.14:5432/pings_2015_to_2017') \
.option("user", "db_user") \
.option("password", "look_at_data") \
.save(mode=saveMode)



# from within postgres:
# \l # lists databses available
# \c <databse-name>; connect to named database
# \d <databse-name>; see schema for named database
# SELECT "PORT_NAME" FROM PINGS_DB LIMIT 10 