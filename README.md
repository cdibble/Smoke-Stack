# ship-soot
Building an Inventory of shipping emissions at ports

![2016_USA_ShippingHeatmap](/img/Coast_Guard_Terrestrial_USEEZContinental_AllShips_2016_Heatmaps_PREVIEW.png)

## Table of Contents
1. [Overview](Readme.md$Overview)
1. [Engineering Challenges](Readme.md$Engineering-Challenges)

how to get the preview?

## Overview
Global trade relies on the constant movement of goods by maritime shipping, but the vessels involved generate a great deal of pollution. In addition to carbon emissions, they produce sulphate and nitrate pollutants and particulate matter. This pollution is known to affect air quality in port regions and recent regulations on the fuels used by shipping vessels are aimed at reducing the negative impacts of pollution from ships while they are in ports.

https://www.sciencedirect.com/science/article/pii/S0048969716327851

Automatic Identification System (AIS), which tracks the movement of ships using ship-to-shore radio, has been required on large commercial vessels in since 2002. With AIS, it is possible to inventory the emissions of ships while in port and to track trends in air quality in port areas related to shipping activity.

## Engineering Challenges
1. Batch process to gather AIS vessel locations.
1. Filtering to vessels in port areas (geospatial query with a radius around a point).
1. Join air quality data from AirNow.
1. Join weather data from NOAA (air temperature, wind speed, precipitation).
1. Store in database.
1. Compute cumulative time in port, estimated emissions, air quality and weather summary data for each vessel-visit.
	1. Delimit a single port visit by a single vessel, store with identifier (maybe just arrival date).
1. Store vessel visit emission inventory	
1. Expose via API:
	1. Query veseel name, port name, date range.
	1. Summarize cumulative emissions per vessel and port.

## Tech Stack