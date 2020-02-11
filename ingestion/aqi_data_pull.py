#!/bin/bash
# aqi_data_pull.sh
######################################################
# WARNING: API CREDENTIALS EXPOSED // USE .gitignore #
######################################################
# Get parameters available for various classes.
# https://aqs.epa.gov/data/api/list/parametersByClass?email=cddibble@gmail.com&key=silverhawk46&pc=CRITERIA
# params
# 14129 lead
# 42101 CO
# 42401 so2
# 42602 no2
# 44201 ozone
# 81102 PM10 total
# 85129 lead PM10
# 88101 PM2.5
# 42101,42401,42602,81102,88101

# Get information about AQI Monitoring Locations using a lat/lon bounding box:
https://aqs.epa.gov/data/api/monitors/byBox?email=cddibble@gmail.com&key=silverhawk46&param=44201&bdate=19950101&edate=19951231&minlat=33.3&maxlat=33.6&minlon=-87.0&maxlon=-86.7

# Get AQI Sample Data By lat/long bounding box
# Up to 5 param args may be given, separated by commas
import requests
def get_aqi(year, minlat, maxlat, minlon, maxlon): # pulls a full year of data for a location.
	request_string = (f"https://aqs.epa.gov/data/api/sampleData/byBox?email=cddibble@gmail.com&key=silverhawk46&param=42101,42401,42602,81102,88101&bdate={year}0101&edate={year}1231&minlat={minlat}&maxlat={maxlat}&minlon={minlon}&maxlon={maxlon}'")
	aqi_pull = requests.get(request_string)
	return(aqi_pull)

# Get ports
source_bucket_dir_ports = "s3a://major-us-ports-csv/"
source_file_name_ports = "geoPorts_v2.parquet"
ports = sqlContext.read.parquet(source_bucket_dir_ports + source_file_name_ports) # Fastest by an order of mag

