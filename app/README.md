# Application for Smoke Stack

The Smoke Stack GUI and API were built in Python using the Flask microserver framework.

The GUI was designed to provide a simple mechanism with which to view trends in the data. Currently it supports viewing invidiual ports or individual ships using drop-down menus. Future updates will include multiple-selectin and map drag-select support.

The API was designed to provide data access to enable analyses and custom visualizations. Details on API endpoints can be found in the [README.md](/README.md#User-Guide) file in the root directory of this repository.

In this directory, the following scripts support the front-end:

1. AIS_Pings_App.py
	1. Flask GUI and API endpoints and functions to compute aggregations and plots.
1. /templates
	1. HTML templates for GUI pages
	1. See: [Templates Readme](/app/templates/README.md)
1. /static
	1. Javascript and CSS files to support Flask framework
	1. See: [Static Readme](/app/static/README.md) 
