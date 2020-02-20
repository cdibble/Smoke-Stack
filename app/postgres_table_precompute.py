# postgres_table_precompute.py
# PostgreSQL : Precomputed Summary Tables for faster queries

from flask import Flask, escape, render_template, request
import psycopg2
import matplotlib.pyplot as plt
import io
import base64
from urllib.parse import quote
####
def get_db():
	# Database for testing is : pings_db_one_month
	# connection = psycopg2.connect("dbname=pings_db_one_month user=db_user password=look_at_data host=44.232.197.79 port=5432")
	# Database for production is : pings_2015_to_2017
	connection = psycopg2.connect("dbname=pings_2015_to_2017 user=db_user password=look_at_data host=44.232.197.79 port=5432")
	return connection	

#### 0.0 : Add categorical VesselType to pings_db
con.close()
con = get_db()
curs = con.cursor()
# curs.execute('DROP TABLE pings_db_withVC') # drop if exists
curs.execute(''' CREATE TABLE pings_db_withVC AS SELECT *,
	CASE WHEN "VesselType"::int IN (30, 1001, 1002) THEN 'Fishing'
	WHEN "VesselType"::int IN (21, 22, 21, 32, 52, 1023, 1025) THEN 'Tug'
	WHEN "VesselType"::int IN (36, 37, 1019) THEN 'Pleasure'
	WHEN "VesselType"::int IN (1021) THEN 'Military'
	WHEN "VesselType"::int IN (60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 1012, 1013, 1014, 1015) THEN 'Passenger'
	WHEN "VesselType"::int IN (70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 1003, 1004, 1016) THEN 'Cargo'
	WHEN "VesselType"::int IN (80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 1017, 1024) THEN 'Tanker'
	ELSE 'Other'
	END "VesselCategory"
	FROM pings_final_in_port_with_visitsubgroup_noudf
	''')
con.commit()

#### 0.1: Compute unique vessel names:
con.close()
con = get_db()
curs = con.cursor()
# curs.execute('DROP TABLE unique_vessel_names2') # drop if exists
# curs.execute(''' CREATE TABLE unique_vessel_names AS SELECT "VesselName", count(DISTINCT "VesselName") from pings_db_withVC group by 1,2 ''')
curs.execute(''' CREATE TABLE unique_vessel_names2 AS
	SELECT "VesselName" from pings_db_withVC
	where "VesselCategory" not in ('Other') group by 1 ''')
# curs.execute(''' CREATE TABLE unique_vessel_names AS SELECT DISTINCT "VesselName" FROM pings_db ''')
con.commit()


#### 1 : aggregate_by_port_ships_per_date():
# TABLE GROUPS BY PORT AND COUNTS THE SHIPS PER DATE (YYYYMMDD) FOR EACH PORT
con = get_db()
curs = con.cursor()
#### *** move to postgresql file :: pre-compute this table for faster queries.
# curs.execute(" DROP TABLE daily_ships_table") # drop if exists
# curs.execute('''CREATE TABLE daily_ships_table AS SELECT DATE("BaseDateTime") , "PORT_NAME", "VesselCategory", count(DISTINCT "MMSI") FROM pings_db_withVC GROUP BY 1 , 2 , 3''')
curs.execute('''CREATE TABLE daily_ships_table AS 
	SELECT DATE("BaseDateTime") , "PORT_NAME", "VesselCategory", count(DISTINCT "VesselName")
	FROM pings_db_withVC GROUP BY 1 , 2 , 3''')
con.commit()
####


#### 2 : aggregate_ship_visits_per_port():
# TABLE GROUPS BY SHIP AND COUNTS THE VISITS PER PORT
con = get_db()
curs = con.cursor()
# curs.execute(" DROP TABLE ships_per_port_table ") # drop if exists
curs.execute('''CREATE TABLE ships_per_port_table AS
	SELECT "VesselName", "PORT_NAME", count(DISTINCT "subgroup")
	FROM pings_db_withVC
	GROUP BY 1 , 2 ''')
con.commit()
####

#### 3 : ship_visit_time_by_port_and_visit():
# TABLE GROUPS BY SHIP, PORT_NAME, and subgroup (== visit) AND AGGREGATES VISIT_TIME
con = get_db()
curs = con.cursor()
# curs.execute(" DROP TABLE ship_visit_time ") # drop if exists
# using extract EPOCH from difftime gives time in seconds.
curs.execute('''CREATE TABLE ship_visit_time AS
	SELECT "VesselName", "PORT_NAME", "subgroup", "VesselCategory", "Length", "Width", "Draft",
	MIN("BaseDateTime") as "Entry_Time", MAX("BaseDateTime") as "Exit_Time",
	EXTRACT(EPOCH FROM MAX("BaseDateTime") - MIN("BaseDateTime")) as "Visit_Time"
	FROM pings_db_withVC
	GROUP BY 1 , 2, 3, 4, 5, 6, 7 ''')
con.commit()
####
#### 3 B: ship_visit_time_by_port():
# TABLE GROUPS BY SHIP, PORT_NAME, and subgroup (== visit) AND AGGREGATES VISIT_TIME
con = get_db()
curs = con.cursor()
# curs.execute(" DROP TABLE ship_visit_total_time ") # drop if exists
curs.execute('''CREATE TABLE ship_visit_total_time AS
	SELECT "VesselName", "PORT_NAME", "VesselCategory",
	"Length", "Width", "Draft", SUM("Visit_Time") AS "Total_Visit_Time"
	FROM ship_visit_time
	GROUP BY 1, 2, 3, 4, 5, 6 ''')
con.commit()
####

#### 3 C: ship_visit_time_by_port_quarterly():
# TABLE GROUPS BY SHIP, PORT_NAME, and subgroup (== visit) AND AGGREGATES VISIT_TIME
con = get_db()
curs = con.cursor()
# curs.execute(" DROP TABLE ship_visit_quarterly ") # drop if exists
curs.execute('''CREATE TABLE ship_visit_quarterly AS
	SELECT "PORT_NAME", "VesselCategory",
	(EXTRACT(YEAR FROM "Entry_Time")+1) - (-0.25*EXTRACT(QUARTER FROM "Entry_Time") + 1.25) as "Quarter",
	SUM("Visit_Time") AS "Total_Visit_Time"
	FROM ship_visit_time
	GROUP BY 1, 2, 3 ''')
con.commit()
####

# daily_ships_table_name
# ship_visit_total_time
# ship_visit_quarterly
