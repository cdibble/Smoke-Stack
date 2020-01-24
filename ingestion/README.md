# Ingestion

AIS data was in ESRI Geodatabase (.gdb) format from 2009 to ~2015, then .csv after that. These files contain functions and execution scripts to assemble all of the data into a single set of tables (one for Pings and one for Vessels) in Parquet format, which will allow for efficient distributed processing in subsequent steps.

1. Parse files S3 bucket containing raw AIS data:
	2. List .gdb folders
	2. List .csv files
1. Extract .gdb data
	3. Convert to Spark dataframes
	3. then write to S3 in Parquet format
1. Read .csv data as Spark dataframes
	4. then write to S3 in Parquet format


