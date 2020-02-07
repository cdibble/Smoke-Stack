from flask import Flask, escape, render_template, request
import psycopg2
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
# run app with self-call
if __name__ == '__main__':
	app.run()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:password@localhost:5432/pings_db'
app.config['DEBUG'] = True

db = SQLAlchemy(app)
db.init_app(app)

POSTGRES = {
	'user': 'postgres',
	'pw': 'look_at_data',
	'db': 'pings_db',
	'host': '10.0.0.14',
	'port': '5432',
}
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % POSTGRES

db.init_app(app)
# Root path that returns home.html
@app.route('/')
def home():
	DATABASE_URI = 'postgres+psycopg2://postgres:password@localhost:5432/pings'
	g.db = psycopg2.connect(host = 'ec2-44-232-197-79.compute-1.amazonaws.com',
						 database = 'pings_db',
						 user = 'postgres',
						 password = 'look_at_data')
	return render_template("home.html")

# About page
@app.route('/about')
def about():
	return render_template("about.html")

# API 
@app.route('/portQuery/<PORT_NAME>')
def get_port(portName):

# Route that accepts a username(string) and returns it. You can use something similar
#   to fetch details of a username
@app.route('/user/<username>')
def show_user_profile(username):
	# show the user profile for that user
	return 'User %s' % escape(username)

@app.route('/userwithslash/<username>/')
def show_user_profile_withslash(username):
	# show the user profile for that user
	return 'User %s' % escape(username)

@app.route('/post/<int:post_id>')
def show_post(post_id):
	# show the post with the given id, the id is an integer
	return 'Post %d' % post_id

@app.route('/path/<path:subpath>')
def show_subpath(subpath):
	# show the subpath after /path/
	return 'Subpath %s' % escape(subpath)


###### Schema / Models
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date

Base = declarative_base()

class Ping(Base):
	__tablename__ = 'pings'    
	MMSI = Column(String, primary_key = True)
	BaseDateTime = Column(Date)
	LAT = Column(String)
	LON = Column(String)
	SOG = Column(String)
	COG = Column(String)
	Heading = Column(String)
	VesselName = Column(String)
	VesselType = Column(String)
	Status = Column(String)
	Length = Column(String)
	Width = Column(String)
	Draft = Column(String)
	Cargo = Column(String)
	dt = Column(Integer)
	subgroup = Column(Integer)
	visit_index = Column(Date)
	PORT_NAME = Column(String)
	def __init__(self, name, author, published):
		self.MMSI = MMSI
		self.BaseDateTime = BaseDateTime
		self.LAT = LAT
		self.LON = LON
		self.SOG = SOG
		self.COG = COG
		self.Heading = Heading
		self.VesselName = VesselName
		self.VesselType = VesselType
		self.Status = Status
		self.Length = Length
		self.Width = Width
		self.Draft = Draft
		self.Cargo = Cargo
		self.dt = dt
		self.subgroup = subgroup
		self.visit_index = visit_index
		self.PORT_NAME = PORT_NAME

	def __repr__(self):
		return '<id {}>'.format(self.id)
	
	def serialize(self):
		return {
			'MMSI' : self.MMSI = MMSI
			'BaseDateTime' : self.BaseDateTime
			'LAT' : self.LAT
			'LON' : self.LON
			'SOG' : self.SOG
			'COG' : self.COG
			'Heading' : self.Heading
			'VesselName' : self.VesselName
			'VesselType' : self.VesselType
			'Status' : self.Status
			'Length' : self.Length
			'Width' : self.Width
			'Draft' : self.Draft
			'Cargo' : self.Cargo
			'dt' : self.dt
			'subgroup' : self.subgroup
			'visit_index' : self.visit_index
			'PORT_NAME' : self.PORT_NAME
		}

	def __repr__(self):
		return "<Ping(title='{}', author='{}', pages={}, published={})>"\
				.format(self.title, self.author, self.pages, self.published)

