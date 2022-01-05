from util import get_connection

def read_table(db_details,table_name,limit=0):
	SOURCE_DB=db_details['SOURCE_DB']

	connection=get_connection(dbname=SOURCE_DB['DB_NAME'],
									dbuser=SOURCE_DB['DB_USER'],
									dbpass=SOURCE_DB['DB_PASS'],
									dbhost=SOURCE_DB['DB_HOST'],dbtype=SOURCE_DB['DB_TYPE'])

	cursor=connection.cursor()

	if limit==0:
		query=f'SELECT * FROM {table_name}'

	else:
		query=f'SELECT * FROM {table_name} LIMIT {limit}'

	cursor.execute(query)
	data=cursor.fetchall()

	column_names=cursor.column_names

	connection.close()

	return data, column_names