import sys
from config import DB_DETAILS
from util import get_tables, load_db_details
from read import read_table
from write import build_insert_query, load_table

def main():
	print("hello world")
	env=sys.argv[1]
	db_details=load_db_details(env)
	tables=get_tables('table_list')
	for table_name in tables['table_name']:
		print(f'reading data for table {table_name}')
		data, column_names = read_table(db_details, table_name,100)
		print(f'loading data for table {table_name}')
		load_table(db_details,data,column_names,table_name)





if __name__=='__main__':
	main()