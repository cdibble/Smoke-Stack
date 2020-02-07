import click
from flask import current_app, g
from flask.cli import with_appcontext
import psycopg2

def get_db():
	def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(host = 'ec2-44-232-197-79.compute-1.amazonaws.com',
                                 database = 'pings_db',
                                 user = 'postgres',
                                 password = 'look_at_data')
        # g.db.row_factory = sqlite3.Row
    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()
	
def init_db():
    db = get_db()

    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))


@click.command('init-db')
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')
