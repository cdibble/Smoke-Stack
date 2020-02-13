# Ingestion

AIS data was in ESRI Geodatabase (.gdb) format from 2009 to 2014 and in .csv after that. These files contain functions and execution scripts to assemble all of the data into a single table in .parquet format, which will allow for efficient distributed processing in subsequent steps.

1. pySpark_DB_Extract.py
	1. Pulls data from S3 lake with an explicit schema, formats timestamp, adds year and month columns, writes to a parquet file.
1. ports_to_geoSpark.py
	1. Pulls [ports data](/small_data), adds 20 km buffer polygon (buffered in local projection to avoid distortion with latitude), writes to parquet.
1. Shell scripts
	1. spark_submit_extraction_.sh : commands to start spark cluster and submit pySpark_DB_Extract.py
	1. spark_submit_ports_geoParquet.sh : commands to start spark cluster and submit ports_to_geoSpark.py
1. schema.py
	1. AIS .csv import data schema.
1. aqi_data_pull.py
	1. Script for matching ports to air quality stations and pulling AQI data.
1. extract_gdb_to_parquet_functions.py
	1. Under development - parsing .gdb files to parquet


