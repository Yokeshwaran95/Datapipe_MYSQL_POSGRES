from util import get_connection


def build_insert_query(table_name,column_names):
	column_values=tuple(map(lambda column: column.replace(column, '%s'), column_names))
	column_names=', '.join(column_names)
	column_values=', '.join(column_values)
	query=(f'''
			INSERT INTO {table_name} ({column_names}) VALUES ({column_values})
		''')

	return query


def insert_data(connection, cursor, query, data, batch_size=100):
	recs=[]
	count=1
	for rec in data:
		recs.append(rec)
		if count % batch_size == 0:
			cursor.executemany(query,recs)
			connection.commit()
			recs=[]
		count=count+1
	cursor.executemany(query,recs)
	connection.commit()
	return 


def load_table(db_details,data, column_names,table_name):
	TARGET_DB=db_details['TARGET_DB']

	connection=get_connection(dbname=TARGET_DB['DB_NAME'],
									dbuser=TARGET_DB['DB_USER'],
									dbpass=TARGET_DB['DB_PASS'],
									dbhost=TARGET_DB['DB_HOST'],dbtype=TARGET_DB['DB_TYPE'])

	cursor=connection.cursor()
	query=build_insert_query(table_name,column_names)
	insert_data(connection, cursor,query, data)
	connection.close()

	