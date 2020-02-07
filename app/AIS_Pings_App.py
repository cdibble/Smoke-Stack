from flask import Flask, escape, render_template, request
import psycopg2
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

# Root path that returns home.html
@app.route('/')
@app.route('/home')
def home():
	# connection = psycopg2.connect("dbname=pings_db user=db_user password=look_at_data host=44.232.197.79 port=5432")
	# cursor = connection.cursor()
	# cursor.execute('''SELECT DISTINCT "PORT_NAME" FROM pings_db ''')
	# port_list = cursor.fetchall()
	return render_template("home.html", port_list = port_list)

@app.route('/port_lat_lon', methods=['GET', 'POST'])
def port_vessels():
	if request.method == "POST":
		port = request.form.get("port")
		connection = get_db() #psycopg2.connect("dbname=pings_db user=db_user password=look_at_data host=44.232.197.79 port=5432")
		cursor = connection.cursor()
		print(str(port))
		lat, lon = get_port_coords(port)
		# cursor.execute('SELECT "VesselName" FROM pings_db WHERE "PORT_NAME"=%(port)s;', %port)
	return render_template("home.html", port_list = port_list, port = port, lat = lat, lon = lon)


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