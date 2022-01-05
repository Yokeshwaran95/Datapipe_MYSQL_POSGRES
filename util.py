import pandas as pd 
from config import DB_DETAILS
from mysql import connector as mc
from mysql.connector import errorcode as ec
import psycopg2




def load_db_details(env):
	return DB_DETAILS[env]

def get_mysql_connection(dbhost,dbname,dbuser,dbpass):
	try:
		conn=mc.connect(user=dbuser,
						password=dbpass,
						host=dbhost,
						database=dbname)
	except mc.Error as error:
		if error.errno==ec.ER_ACCESS_DENIED_ERROR:
			print("Invalid credentials")
		else:
			print(error)

	return conn

def get_pg_connection(dbhost,dbname,dbuser,dbpass):
	connection=psycopg2.connect(f'dbname={dbname} user={dbuser} host={dbhost} password={dbpass}')
	return connection


def get_connection(dbhost,dbname,dbuser,dbpass,dbtype):
	connection=None
	if dbtype=='mysql':
		connection=get_mysql_connection(dbhost=dbhost,
										dbname=dbname,
										dbuser=dbuser,
										dbpass=dbpass)
	elif dbtype=='postgres':
		connection=get_pg_connection(dbhost=dbhost,
										dbname=dbname,
										dbuser=dbuser,
										dbpass=dbpass)		
	return connection





def get_tables(path):
	tables=pd.read_csv(path,sep=':')
	return tables.query("to_be_loaded =='yes'")