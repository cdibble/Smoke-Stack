#!/usr/bin/env python3
from pyspark.sql.types import *
# Import to runner / Extract process 

schema = StructType(List(
    StructField('MMSI',StringType,true),
    StructField('BaseDateTime',StringType,true),
    StructField('LAT',StringType,true),
    StructField('LON',StringType,true),
    StructField('SOG',StringType,true),
    StructField('COG',StringType,true),
    StructField('Heading',StringType,true),
    StructField('VesselName',StringType,true),
    StructField('IMO',StringType,true),
    StructField('CallSign',StringType,true),
    StructField('VesselType',StringType,true),
    StructField('Status',StringType,true),
    StructField('Length',StringType,true),
    StructField('Width',StringType,true),
    StructField('Draft',StringType,true),
    StructField('Cargo',StringType,true)
))