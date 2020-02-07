from flask import Flask, escape, render_template, request
import psycopg2
# from flask_sqlalchemy import SQLAlchemy
app = Flask(__name__)


# run app with self-call
# if __name__ == '__main__':

# Root path that returns home.html
@app.route('/')
def home():
	return render_template("home.html")

# @app.route('/')
# def home():
# 	return render_template("home.html")

# DATABASE_URI - open connection
# db = psycopg2.connect("dbname=pings_db user=db_user password=look_at_data host=44.232.197.79 port=5432")

# About page
# @app.route('/about')
# def about():
# 	return render_template("about.html")

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


###### Schema / Models
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date

from flask_sqlalchemy import SQLAlchemy
import datetime

# db = SQLAlchemy()

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




# Base = declarative_base()
# class Ping(Base):
# 	__tablename__ = 'pings'    
# 	MMSI = Column(String, primary_key = True)
# 	BaseDateTime = Column(Date)
# 	LAT = Column(String)
# 	LON = Column(String)
# 	SOG = Column(String)
# 	COG = Column(String)
# 	Heading = Column(String)
# 	VesselName = Column(String)
# 	VesselType = Column(String)
# 	Status = Column(String)
# 	Length = Column(String)
# 	Width = Column(String)
# 	Draft = Column(String)
# 	Cargo = Column(String)
# 	dt = Column(Integer)
# 	subgroup = Column(Integer)
# 	visit_index = Column(Date)
# 	PORT_NAME = Column(String)
# 	def __init__(self, name, author, published):
# 		self.MMSI = MMSI
# 		self.BaseDateTime = BaseDateTime
# 		self.LAT = LAT
# 		self.LON = LON
# 		self.SOG = SOG
# 		self.COG = COG
# 		self.Heading = Heading
# 		self.VesselName = VesselName
# 		self.VesselType = VesselType
# 		self.Status = Status
# 		self.Length = Length
# 		self.Width = Width
# 		self.Draft = Draft
# 		self.Cargo = Cargo
# 		self.dt = dt
# 		self.subgroup = subgroup
# 		self.visit_index = visit_index
# 		self.PORT_NAME = PORT_NAME

# 	def __repr__(self):
# 		return '<id {}>'.format(self.id)
	
# 	def serialize(self):
# 		return {
# 			'MMSI' : self.MMSI = MMSI
# 			'BaseDateTime' : self.BaseDateTime
# 			'LAT' : self.LAT
# 			'LON' : self.LON
# 			'SOG' : self.SOG
# 			'COG' : self.COG
# 			'Heading' : self.Heading
# 			'VesselName' : self.VesselName
# 			'VesselType' : self.VesselType
# 			'Status' : self.Status
# 			'Length' : self.Length
# 			'Width' : self.Width
# 			'Draft' : self.Draft
# 			'Cargo' : self.Cargo
# 			'dt' : self.dt
# 			'subgroup' : self.subgroup
# 			'visit_index' : self.visit_index
# 			'PORT_NAME' : self.PORT_NAME
# 		}

# 	def __repr__(self):
# 		return "<Ping(title='{}', author='{}', pages={}, published={})>"\
# 				.format(self.title, self.author, self.pages, self.published)

