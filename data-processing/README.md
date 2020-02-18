# Data Processing

Data were processed using Spark on an AWS EC2 cluster. Raw data sources were AIS ship pings and US Port locations.

A 20 km buffer was applied around each port location in a local projection (avoiding distortion with changing latitude). Ship pings were then placed in potential port areas using a geoHashing approach (see slides for details). Geohashing allowed for fast geospatial assignments.

Pings were indexed for "visits" for a given ship, port, and time period. This was done by computing the time difference between pings and then flagging time gaps greater than an arbitrary cutoff (chosen as 48-hrs). A subsequent rolling cumulative sum creates an index of visits for each ship and port.

The data were then written as Postgres tables for querying.

These steps were accomplished with a single script:

1. make_geoPings_join_ports.py
	1. Joins pings with ports, tests whether pings are truly in (using a UDF and shapely calls, so this is potentially expensive and can be omitted), computes visit index and writes to Postgres.

Other files are included with results and plots from a benchmarking experiment to better understand the impacts of the geoHashing process.

1. geoHash_BenchMark_Experiment.csv
	1. Results from checks on effect of geoHash mesh size on ping-check savings.
1. geoHash_benchmarking_plots.R
	1. Script to build plot of benchmarking experiment results.
1. geoHash_benchmarking_figure.pdf
	1. Figuring displaying benchmarking experiment results.
1. port_polygons_check.csv
	1. Contains all of the polygons circumscribing ports with a 20km radius.
1. port_polygons_map.png
	1. Map displaying ports as polygons.