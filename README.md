# ship-soot
Building an Inventory of shipping emissions at ports

## Overview
Global trade relies on the constant movement of goods by maritime shipping, but the vessels involved generate a great deal of pollution. In addition to carbon emissions, they produce sulphate and nitrate pollutants and particulate matter. This pollution is known to affect air quality in port regions and recent regulations on the fuels used by shipping vessels are aimed at reducing the negative impacts of pollution from ships while they are in ports.

https://www.sciencedirect.com/science/article/pii/S0048969716327851

Automatic Identification System (AIS), which tracks the movement of ships using ship-to-shore radio, has been required on large commercial vessels in since 2002. With AIS, it is possible to inventory the emissions of ships while in port and to track trends in air quality in port areas related to shipping activity.

## Engineering Challenges
Batch process to gather AIS vessel locations.
Filtering to vessels in port areas (geospatial query with a radius around a point).
Join air quality data from AirNow.
Join weather data from NOAA (air temperature, wind speed, precipitation).
Store in database.
Compute cumulative time in port, estimated emissions, air quality and weather summary data for each vessel-visit.
	Delimit a single port visit by a single vessel, store with identifier (maybe just arrival date).
Store vessel visit emission inventory	
Expose via API:
	Query veseel name, port name, date range.
	Summarize cumulative emissions per vessel and port.

## Tech Stack