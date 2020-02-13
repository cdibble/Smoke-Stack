# Smoke Stack
Estimate ship emissions inventories and air quality trends in ports to lower supply chain costs, reduce public health and environmental footprints, and demonstrate successful sustainability initiatives.

<hr/>

![2016_USA_ShippingHeatmap](/img/Coast_Guard_Terrestrial_USEEZContinental_AllShips_2016_Heatmaps_PREVIEW.png)

<hr/>

## Table of Contents
1. [Overview](Readme.md$Overview)
1. [Work Flow](Readme.md$Work-Flow)
1. [Coming Soon](Readme.md$Coming-Soon-(in-order-of-priority))
1. [User Guide](Readme.md$User-Guide)

## Overview
Global trade relies on the constant movement of goods by maritime shipping, but the vessels involved generate a great deal of pollution. In addition to carbon emissions, they produce sulphate and nitrate pollutants and particulate matter. This pollution is known to affect air quality in port regions and a patchwork of regulations has unfolded to address the issue.

https://www.sciencedirect.com/science/article/pii/S0048969716327851

Automatic Identification System (AIS), which tracks the movement of ships using ship-to-shore radio, has been required on large commercial vessels in since 2002. With AIS, it is possible to inventory the emissions of ships while in port and to track trends in air quality in port areas related to shipping activity.

## User Guide
Smoke Stack exposes the data via a simple GUI as well as through a RESTful API. They are described briefly below. For more information about building the project and the implementation details, [engineering details](Readme.md$Engineering-Details) is a good place to start.

### GUI
Users can view records in two ways: "Select Port" or "Select Ship". For each, a drop down menu contains options that can be queried via the "Submit" button.

"Select Port" will display a map with the port location and two plots. The first displays the number of ships in that port each day. The second diplays the total visit time in days for different vessel categories for each quarter-year (Cargo, Tanker, Passenger, Fishing, Pleasure, Other).

"Select Ship" displays two plots for the selected vessel. The first shows the total number of visits the ship has made to each of the ports that it has visited. The second shows the cumulative visit time in days that the ship has spent at various ports.

Coming features: Concurrent air quality trends, regression with air quality and visit time scaled by ship tonnage, emissions estimates based on visit time and ship tonnage. See "Coming Soon" below for more.

### API
Users can access data via a RESTful API with several different endpoints. They are listed first with examples below.

1. ['/smokestackAPI/v1.0/port_query_shipsPerDay/<port>'](Readme.md$1)
1. ['/smokestackAPI/v1.0/port_query_visitTimeQuarterly/<port>'](Readme.md$2)
1. ['/smokestackAPI/v1.0/ship_query_visitsPerPort/<ship>'](Readme.md$3)
1. ['/smokestackAPI/v1.0/ship_query_totalTimePerPort/<ship>'](Readme.md$4)

### 1
``` '/smokestackAPI/v1.0/port_query_shipsPerDay/<port>' ```

Value:
Returns the number of ships per day throughout the database record for a given port.

Usage:
Port names generally contain spaces and commas, which must be encoded. Spaces are replaced with `%20` and commas are replaced with `%2c`.

Example:
To query with port = "San Francisco, CA", use "San%20Francisco%2c%20".

	``` curl "http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/port_query_shipsPerDay/San%20Francisco%2c%20CA" ```
### 2
``` '/smokestackAPI/v1.0/port_query_visitTimeQuarterly/<port>' ```
Value:
Returns the cumulative visit time per quarter-year throughout the database record for a given port.

Usage:
Port names generally contain spaces and commas, which must be encoded to be used in a url/uri. Spaces are replaced with `%20` and commas are replaced with `%2c`.

Example:
To query with port = "San Francisco, CA", use "San%20Francisco%2c%20".

	``` curl "http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/port_query_shipsPerDay/San%20Francisco%2c%20CA" ```
### 3
``` '/smokestackAPI/v1.0/ship_query_visitsPerPort/<ship>' ```
Value:
Returns the number of visits to each visited port for a given ship.

Usage:
Ship names generally contain spaces, which must be encoded to be used in a url/uri. Spaces are replaced with `%20`.

Example:
To query with ship = "", use "San%20Francisco%2c%20".

	``` curl "http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/port_query_shipsPerDay/San%20Francisco%2c%20CA" ```
### 4
``` '/smokestackAPI/v1.0/ship_query_totalTimePerPort/<ship>' ```
Value:
Returns the total visit time in days spent at each visited port for a given ship.

Usage:
Ship names generally contain spaces, which must be encoded to be used in a url/uri. Spaces are replaced with `%20`.

Example:
To query with ship = "", use "San%20Francisco%2c%20".

	``` curl "http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/ship_query_totalTimePerPort/San%20Francisco%2c%20CA" ```

## Work Flow
1. Extract from US Gov server (marinecadastre.gov) to S3 data lake
1. Buffer (in a local projection) Major_US_Ports with a radius of 20 km.
1. GeoHash (to reduce subsequent queries) then test whether ship pings are in a port.
1. Apply rolling cumulative sum of binary flags when a gap in pings > 48 hrs occurs; when applied with a window over data partitioned by port and vessel name, this yeilds an index of ship visits. The key assumption is that ship visits are marked by gaps between pings > 48 hrs.
1. Write data to PostgreSQL database
1. Build aggregate tables to enable fast front-end and API queries
1. Expose data via API and GUI using Flask.

## Tech Stack
1. AWS Cloud Infrastructure
1. pySpark for distributed ingestion and processing
1. PostgreSQL for database storage and queries
1. Flask for API and GUI frontends.

## Coming Soon (in order of priority)
1. Join air quality data from aqs.epa.gov.
1. Model emissions inventory.
1. Allow time range filtering for all queries.
1. Implement authentication for API.
1. Build out PostGIS geospatial queries with map drag.
1. Join weather data from NOAA (air temperature, wind speed, precipitation).
1. Data from 2012-2015, extracted from .gdb format.

## Engineering Details
The project was implemented in stages that reflect the directory structure of this repository: [ingestion](/ingestion), [data processing](/data-processing), [database build](/database-scripts), and [data exposure](/app).

Spark did the distributed processing needed to extract the data from its home server (marinecadastre.gov), store it in an S3 data lake, and apply geo-hashing and processing steps. Intermediate data structures were stored in S3 as parquet files awaiting appends from the roughly five years of earlier data that is accessible only in ESRI geodatabase (.gdb) format. 

The processed data were stored in a PostgreSQL database hosted on AWS EC2 and pre-processed tables for the app were built using SQL queries.

Flask, a python microserver framework, was used to build a RESTful API and GUI to expose the data to end users.