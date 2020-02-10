from flask import Flask, escape, render_template, request
import psycopg2
import matplotlib.pyplot as plt
import io
import base64
from urllib.parse import quote
# from flask_sqlalchemy import SQLAlchemy
app = Flask(__name__)

# run app with self-call
# if __name__ == '__main__':

def get_db():
	connection = psycopg2.connect("dbname=pings_db_one_month user=db_user password=look_at_data host=44.232.197.79 port=5432")
	return connection	

def get_port_list():
	con = get_db()
	curs = con.cursor()
	curs.execute('''SELECT DISTINCT "PORT_NAME" FROM ports_db''')
	port_list = curs.fetchall()
	port_list = [port[0] for port in port_list]
	return port_list

port_list = get_port_list()

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

def aggregate_by_port_ships_per_date(PORT_NAME):
	# Note create index for PORT_NAME to speed up quieries
	con = get_db()
	curs = con.cursor()
	# v1 Extract day from datetime, count distinct vessels per day at a port
	# curs.execute(''' SELECT EXTRACT(Day FROM "BaseDateTime") , count(DISTINCT "VesselName") FROM pings_db WHERE "PORT_NAME" = '{}' GROUP BY 1 '''.format(PORT_NAME))
	# curs.execute(''' SELECT DATE("BaseDateTime") , count(DISTINCT "MMSI") FROM pings_db WHERE "PORT_NAME" = '{}' GROUP BY 1 '''.format(PORT_NAME))
	# daily_ships_in_port = curs.fetchall()
	# v2 Extract day from datetime, count distinct vessels per day at a port
	# curs.execute(''' SELECT EXTRACT(Day FROM "BaseDateTime") , count(DISTINCT "VesselName") FROM pings_db WHERE "PORT_NAME" = '{}' GROUP BY 1 '''.format(PORT_NAME))
	curs.execute(''' SELECT "date", "count" FROM daily_ships_table WHERE "PORT_NAME" = '{}' '''.format(PORT_NAME))
	daily_ships_in_port = curs.fetchall()
	return daily_ships_in_port
# aggregate_by_port_ships_per_date('San Francisco, CA')
# aggregate_by_port_ships_per_date('Port Fourchon, LA')

	# curs.execute(''' SELECT EXTRACT(Day FROM "BaseDateTime") , count(DISTINCT "VesselName") FROM pings_db WHERE "PORT_NAME" = 'San Francisco, CA' GROUP BY 1 '''.format(PORT_NAME))
	# curs.execute(''' SELECT DISTINCT "VesselName" FROM pings_db WHERE "PORT_NAME" = 'Oakland, CA' ''')
	# curs.execute(''' SELECT DISTINCT "PORT_NAME" FROM pings_db ''')

### --------- ROUTES --------------- ###
# Root path that returns home.html
@app.route('/')
@app.route('/home')
def home():
	return render_template("home.html", port_list = port_list)

@app.route('/port_lat_lon', methods=['GET', 'POST'])
def port_vessels():
	if request.method == "POST":
		port = request.form.get("port")
		connection = get_db() #psycopg2.connect("dbname=pings_db user=db_user password=look_at_data host=44.232.197.79 port=5432")
		cursor = connection.cursor()
		print(str(port))
		lat, lon = get_port_coords(port)
		plot_url = plot_daily_ships_in_port(port)
		# cursor.execute('SELECT "VesselName" FROM pings_db WHERE "PORT_NAME"=%(port)s;', %port)
	return render_template("home.html", port_list = port_list, port = port, lat = lat, lon = lon, plot_url = plot_url)

# @app.route('/build_plot')
def plot_daily_ships_in_port(PORT_NAME):
	daily_ships = aggregate_by_port_ships_per_date(PORT_NAME)
	# daily_ships = daily_ships_in_port
	days = [daily_ships[x][0] for x in range(len(daily_ships)) ]
	ships = [daily_ships[x][1] for x in range(len(daily_ships)) ]
	print(days)
	print(ships)
	# ship_plot = plt.plot(days, ships)
	ship_plot, ax = plt.subplots()
	ax.plot(days, ships)
	ax.set(xlabel='time (days)', ylabel='number of ships', title='Ship Visits At Selected Port')
	ax.grid()
	# Encode image:
	plot_img = io.BytesIO()
	ship_plot.savefig(plot_img , format = 'png')
	# ship_plot.savefig('plot1.png'), format='png')
	plot_img.seek(0)
	plot_url = base64.b64encode(plot_img.getvalue()).decode()
	# return plot_url
	# return '<img src="data:image/png;base64,{}"alt="plot_img"/>'.format(quote(plot_url))
	return "data:image/png;base64,{}".format(quote(plot_url))
	 
# plot_daily_ships_in_port('Port Fourchon, LA')
	# fig = Figure()
	# axis = fig.add_subplot(1, 1, 1)
	# axis.plot(days, ships)
	# canvas = FigureCanvas(fig)
	# output = StringIO.StringIO()
	# canvas.print_png(output)
	# response = make_response(output.getvalue())
	# response.mimetype = 'image/png'
	# return response


# About page
@app.route('/about')
def about():
	return render_template("about.html")

# @app.route('/port_index')
# def port_index():
# 	connection = psycopg2.connect("dbname=pings_db user=db_user password=look_at_data host=44.232.197.79 port=5432")
# 	cursor = connection.cursor()
# 	cursor.execute('''SELECT DISTINCT "PORT_NAME" FROM pings_db ''')
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