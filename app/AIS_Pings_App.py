# daily_ships = aggregate_by_port_ships_per_date('Port Fourchon, LA')
# daily_ships = pd.DataFrame(daily_ships, columns = ["BaseDateTime", "VesselCategory", "Count"])
from flask import Flask, escape, render_template, request, jsonify
from flask_restful import Resource, Api
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
from urllib.parse import quote
import re # regex ops
# from flask_sqlalchemy import SQLAlchemy
app = Flask(__name__)
api = Api(app)
# run app with self-call
# if __name__ == '__main__':

def get_db():
		# use first for development (one month of data)
	# connection = psycopg2.connect("dbname=pings_db_one_month user=db_user password=look_at_data host=44.232.197.79 port=5432")
	connection = psycopg2.connect("dbname=pings_2015_to_2017 user=db_user password=look_at_data host=44.232.197.79 port=5432")
	return connection	

def get_port_list():
	con = get_db()
	curs = con.cursor()
	curs.execute('''SELECT DISTINCT "PORT_NAME" FROM ports_db''')
	port_list = curs.fetchall()
	port_list = [port[0] for port in port_list]
	port_states = [re.search('[A-Z]{2}', x).group() if re.search('[A-Z]{2}', x) else "None" for x in port_list]
	port_df = pd.DataFrame({"ports" : port_list, "states" : port_states})
	# Fix two ports that don't have state abbreviations
	port_df.loc[port_df['ports'] == "Huntington - Tristate", 'states'] = "WV"
	port_df.loc[port_df['ports'] == "Central Louisiana Regional Port", 'states'] = "LA"
	# sort by state then by port
	port_df = port_df.sort_values(['states', 'ports'])
	port_list = list(port_df['ports'])
	return port_list
port_list = get_port_list()

def get_port_df():
	con = get_db()
	curs = con.cursor()
	#curs.execute('''SELECT "X", "Y", "PORT_NAME", count(DISTINCT "PORT_NAME") FROM ports_db GROUP BY 1, 2, 3 ''')
	curs.execute('''SELECT "X", "Y", "PORT_NAME" FROM ports_db GROUP BY 1, 2, 3 ''')
	port_df = curs.fetchall()
	port_df = pd.DataFrame(port_df, columns = ["LON", "LAT", "PORT_NAME"])
	return port_df
port_df = get_port_df()

def get_ship_list():
	con = get_db()
	curs = con.cursor()
	#curs.execute('''SELECT DISTINCT "VesselName" FROM pings_db_withVC''')
	curs.execute('''SELECT "VesselName" FROM unique_vessel_names2''') # unique_vessel_names2 excludes 'Other' categories.
	ship_list = curs.fetchall()
	ship_list = [ship[0] for ship in ship_list]
	return ship_list
ship_list_overall = get_ship_list()

def get_port_coords(PORT_NAME):
	print(PORT_NAME)
	con = get_db()
	curs = con.cursor()
	curs.execute(''' SELECT "Y", "X" FROM ports_db WHERE "PORT_NAME" = '{}' '''.format(PORT_NAME))
	coords = curs.fetchall()
	lat , lon = coords[0][0], coords[0][1]
	print([lat, lon])
	return [lat, lon]
# get_port_coords('Port Fourchon, LA')

### Aggregate ships at port through time
def aggregate_by_port_ships_per_date(PORT_NAME):
	con = get_db()
	curs = con.cursor()
	curs.execute(''' SELECT "date", "VesselCategory", "count" FROM daily_ships_table WHERE "PORT_NAME" = '{}' '''.format(PORT_NAME))
	daily_ships_in_port = curs.fetchall()
	return daily_ships_in_port
# aggregate_by_port_ships_per_date('San Francisco, CA')
# aggregate_by_port_ships_per_date('Port Fourchon, LA')

def plot_daily_ships_in_port(PORT_NAME):
	daily_ships = pd.DataFrame(aggregate_by_port_ships_per_date(PORT_NAME), columns = ["BaseDateTime", "VesselCategory", "Count"])
	if daily_ships.shape[0] > 0:
		ship_plot, ax = plt.subplots()
		sns.set(style = "whitegrid", rc = {'xtick.bottom':"True", 'ytick.left':"True"})
		ax = sns.lineplot(x="BaseDateTime", y="Count", hue="VesselCategory", data = daily_ships)
		ax.grid()
		ax.set(xlabel='date', ylabel='number of ships', title='Ship Visits At Selected Port')
		ship_plot.autofmt_xdate() # allow x-axis labels to rotate for readability
		# Encode image:
		plot_img = io.BytesIO()
		ship_plot.savefig(plot_img , format = 'png')
		plot_img.seek(0)
		plot_url = base64.b64encode(plot_img.getvalue()).decode()
		# return '<img src="data:image/png;base64,{}"alt="plot_img"/>'.format(quote(plot_url))
		return "data:image/png;base64,{}".format(quote(plot_url))
	else:
		return "data:image/png;base64,()"

#### aggregate ship visits total time by VesselCategory per Port
def aggregate_ship_visits_total_time_quarterly(PORT_NAME):
	con = get_db()
	curs = con.cursor()
	curs.execute(''' SELECT "Quarter", "PORT_NAME", "VesselCategory", "Total_Visit_Time" FROM ship_visit_quarterly WHERE "PORT_NAME" = '{}' '''.format(PORT_NAME))
	daily_ships_in_port = curs.fetchall()
	return daily_ships_in_port
# aggregate_by_port_ships_per_date('Port Fourchon, LA')
# PORT_NAME = 'Port Everglades, FL'

def plot_ship_visits_total_time_quarterly(PORT_NAME):
	ships_visit_time_per_port = pd.DataFrame(aggregate_ship_visits_total_time_quarterly(PORT_NAME), columns = ["Quarter", "PORT_NAME", "VesselCategory", "Total_Visit_Time"])
	# ships_visit_time_per_port = pd.DataFrame(ships_visit_time_per_port, columns = ["Quarter", "PORT_NAME", "VesselCategory", "Total_Visit_Time"])
	if ships_visit_time_per_port.shape[0] > 0:
		ships_visit_time_per_port['Total_Visit_Time'] = ships_visit_time_per_port['Total_Visit_Time'] / (3600*24)
		ship_plot, ax = plt.subplots()
		sns.set(style = "whitegrid", rc = {'xtick.bottom':"True", 'ytick.left':"True"})
		# ax = sns.catplot(x="PORT_NAME", y="Count", hue="VesselName", data = ships_visit_time_per_port, kind = 'bar')
		ax = sns.barplot(x="Quarter", y="Total_Visit_Time", hue= "VesselCategory", data = ships_visit_time_per_port)
		# ax.grid()
		ax.set(xlabel='port', ylabel='total visit time (days)', title= f"Total Visit Time At Port: {PORT_NAME}")
		ship_plot.autofmt_xdate() # allow x-axis labels to rotate for readability
		# Encode image:
		plot_img = io.BytesIO()
		ship_plot.savefig(plot_img , format = 'png')
		plot_img.seek(0)
		plot_url = base64.b64encode(plot_img.getvalue()).decode()
		return "data:image/png;base64,{}".format(quote(plot_url))	
	else:
		return "data:image/png;base64,()"

#### Aggregate ship visits (count) at each port.
# SHIP='Victory'
def aggregate_ship_visits_per_port(SHIP):
	con = get_db()
	curs = con.cursor()
	curs.execute(''' SELECT "VesselName", "PORT_NAME", "count" FROM ships_per_port_table WHERE "VesselName" = '{}' '''.format(SHIP))
	daily_ships_in_port = curs.fetchall()
	return daily_ships_in_port
# aggregate_ship_visits_per_port('DENNIS C BOTTORFF')
# daily_ships = aggregate_by_port_ships_per_date('Port Fourchon, LA')
# daily_ships = pd.DataFrame(daily_ships, columns = ["BaseDateTime", "VesselCategory", "Count"])

def plot_ship_visits_per_port(SHIP):
	ships_per_port = pd.DataFrame(aggregate_ship_visits_per_port(SHIP), columns = ["VesselName", "PORT_NAME", "Count"])
	# ships_per_port = pd.DataFrame(xx, columns = ["VesselName", "PORT_NAME", "Count"])
	if ships_per_port.shape[0] > 0:
		ship_plot, ax = plt.subplots()
		sns.set(style = "whitegrid", rc = {'xtick.bottom':"True", 'ytick.left':"True"})
		# ax = sns.catplot(x="PORT_NAME", y="Count", hue="VesselName", data = ships_per_port, kind = 'bar')
		ax = sns.barplot(x="PORT_NAME", y="Count", data = ships_per_port)
		# ax.grid()
		ax.set(xlabel='port', ylabel='number of visits', title= f"Number of Visits At Each Port For Ship: {SHIP}")
		ship_plot.autofmt_xdate() # allow x-axis labels to rotate for readability
		# Encode image:
		plot_img = io.BytesIO()
		ship_plot.savefig(plot_img , format = 'png')
		plot_img.seek(0)
		plot_url = base64.b64encode(plot_img.getvalue()).decode()
		return "data:image/png;base64,{}".format(quote(plot_url))
	else:
		return "data:image/png;base64,()"

#### Aggregate total ship visit time per port
def aggregate_ship_visits_total_time(SHIP):
	con = get_db()
	curs = con.cursor()
	curs.execute(''' SELECT "VesselName", "PORT_NAME", "VesselCategory", "Total_Visit_Time" FROM ship_visit_total_time WHERE "VesselName" = '{}' '''.format(SHIP))
	daily_ships_in_port = curs.fetchall()
	return daily_ships_in_port
# aggregate_ship_visits_total_time('DENNIS C BOTTORFF')

def plot_ship_visits_total_time(SHIP):
	ships_visit_time_per_port = pd.DataFrame(aggregate_ship_visits_total_time(SHIP), columns = ["VesselName", "PORT_NAME", "VesselCategory", "Total_Visit_Time"])
	# ships_visit_time_per_port = pd.DataFrame(ships_visit_time_per_port, columns = ["VesselName", "PORT_NAME", "VesselCategory", "Total_Visit_Time"])
	if ships_visit_time_per_port.shape[0] > 0:
		ships_visit_time_per_port['Total_Visit_Time'] = ships_visit_time_per_port['Total_Visit_Time'] / (3600*24)
		ship_plot, ax = plt.subplots()
		sns.set(style = "whitegrid", rc = {'xtick.bottom':"True", 'ytick.left':"True"})
		# ax = sns.catplot(x="PORT_NAME", y="Count", hue="VesselName", data = ships_visit_time_per_port, kind = 'bar')
		ax = sns.barplot(x="PORT_NAME", y="Total_Visit_Time", data = ships_visit_time_per_port)
		# ax.grid()
		ax.set(xlabel='port', ylabel='total visit time (days)', title= f"Total Visit Time At Each Port For Ship: {SHIP}")
		ship_plot.autofmt_xdate() # allow x-axis labels to rotate for readability
		# Encode image:
		plot_img = io.BytesIO()
		ship_plot.savefig(plot_img , format = 'png')
		plot_img.seek(0)
		plot_url = base64.b64encode(plot_img.getvalue()).decode()
		return "data:image/png;base64,{}".format(quote(plot_url))
	else:
		return "data:image/png;base64,()"

########################################	
### --------- ROUTES --------------- ###
########################################
# Root path that returns home.html
@app.route('/')
@app.route('/home')
def home():
	return render_template("home.html", port_list = port_list, port_df = port_df, port = "Oakland, CA")

# Take "port" input from drop-down on /home
@app.route('/port_lat_lon', methods=['GET', 'POST'])
def port_vessels():
	if request.method == "POST":
		port = request.form.get("port")
		connection = get_db() #psycopg2.connect("dbname=pings_db_withVC user=db_user password=look_at_data host=44.232.197.79 port=5432")
		cursor = connection.cursor()
		print(str(port))
		lat, lon = get_port_coords(port)
		plot_url = plot_daily_ships_in_port(port)
		total_ship_time_plot_url = plot_ship_visits_total_time_quarterly(port)
	return render_template("home.html", port_df = port_df, port_list = port_list, port = port, lat = lat, lon = lon, plot_url = plot_url, total_ship_time_plot_url = total_ship_time_plot_url)

@app.route('/ship_index', methods=['GET', 'POST'])
def ship_index():
	if request.method == "POST":
		ship = request.form.get("ship")
		connection = get_db() #psycopg2.connect("dbname=pings_db_withVC user=db_user password=look_at_data host=44.232.197.79 port=5432")
		cursor = connection.cursor()
	return render_template("ship_index.html", ship_list = ship_list_overall, port_list = port_list, ship = "Victory")

@app.route('/ship_compute', methods=['GET', 'POST'])
def ship_compute():
	if request.method == "POST":
		ship = request.form.get("ship")
		connection = get_db() #psycopg2.connect("dbname=pings_db_withVC user=db_user password=look_at_data host=44.232.197.79 port=5432")
		cursor = connection.cursor()
		print(str(ship))
		ship_plot_url = plot_ship_visits_per_port(ship)
		ship_plot_total_time_url = plot_ship_visits_total_time(ship)
	return render_template("ship_index.html", ship_list = ship_list_overall, plot_url = ship_plot_url, ship_plot_total_time = ship_plot_total_time_url, ship = ship)

@app.route('/api_documentation')
def api_doc():
	return render_template("api_doc.html")

# About page
@app.route('/about')
def about():
	return render_template("about.html")

########################################	
### --------- API --------------- ###
########################################
@app.route('/smokestackAPI/v1.0/port_query_shipsPerDay/<port>', methods=['GET'])
def api_get_port_table(port):	
	# with app.app_context(): # for development 
	return jsonify(aggregate_by_port_ships_per_date(port))

@app.route('/smokestackAPI/v1.0/port_query_visitTimeQuarterly/<port>', methods=['GET'])
def api_get_port_visits_quarterly(port):
	return jsonify(aggregate_ship_visits_total_time_quarterly(port))

# curl "http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/port_query_shipsPerDay/Port%20Fourchon%2c%20LA"
# curl http://ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000/smokestackAPI/v1.0/port_query_visitTimeQuarterly/Port%20Fourchon%2c%20LA
@app.route('/smokestackAPI/v1.0/ship_query_visitsPerPort/<ship>', methods=['GET'])
def api_get_ship_visits_quarterlyPerPort(ship):
	return jsonify(aggregate_ship_visits_per_port(ship))

@app.route('/smokestackAPI/v1.0/ship_query_totalTimePerPort/<ship>', methods=['GET'])
def api_get_ship_totalTimePerPort(ship):
	return jsonify(aggregate_ship_visits_total_time(ship))

# @app.route('/port_index')
# def port_index():
# 	connection = psycopg2.connect("dbname=pings_db_withVC user=db_user password=look_at_data host=44.232.197.79 port=5432")
# 	cursor = connection.cursor()
# 	cursor.execute('''SELECT DISTINCT "PORT_NAME" FROM pings_db_withVC ''')
# 	port_list = cursor.fetchall()
# 	port_list = sorted(port_list)
# 	return render_template("port_index.html", port_list=port_list)
# # API 
# @app.route('/portQuery/<PORT_NAME>')
# def get_port(portName):

# Route that accepts a username(string) and returns it. You can use something similar
#   to fetch details of a username
# @app.route('/user/<username>')
# def show_user_profile(username):
# 	# show the user profile for that user
# 	return 'User %s' % escape(username)

# @app.route('/userwithslash/<username>/')
# def show_user_profile_withslash(username):
# 	# show the user profile for that user
# 	return 'User %s' % escape(username)

# @app.route('/post/<int:post_id>')
# def show_post(post_id):
# 	# show the post with the given id, the id is an integer
# 	return 'Post %d' % post_id

# @app.route('/path/<path:subpath>')
# def show_subpath(subpath):
# 	# show the subpath after /path/
# 	return 'Subpath %s' % escape(subpath)

# Classes
# class BaseModel(db.Model):
#     """Base data model for all objects"""
#     __abstract__ = True

#     def __init__(self, *args):
#         super().__init__(*args)

#     def __repr__(self):
#         """Define a base way to print models"""
#         return '%s(%s)' % (self.__class__.__name__, {
#             column: value
#             for column, value in self._to_dict().items()
#         })

#     def json(self):
#         """
#                 Define a base way to jsonify models, dealing with datetime objects
#         """
#         return {
#             column: value if not isinstance(value, datetime.date) else value.strftime('%Y-%m-%d')
#             for column, value in self._to_dict().items()
#         }


# class Station(BaseModel, db.Model):
#     """Model for the stations table"""
#     __tablename__ = 'stations'

#     id = db.Column(db.Integer, primary_key = True)
#     lat = db.Column(db.Float)
#     lng = db.Column(db.Float)