import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine #engine в sqlalchemy == connection


#get_config - function that gets parameters for connection to postgres DB
def get_config(path = 'db_config.json'):
	try:
		with open(path, "r") as file:
			config = json.load(file)
			print("Connection parameters have loaded from json")
			return config
	except Exception as error:
		print(error)
		quit()

#get_connection - function that set connection to DB
def get_connection(config):
	connection = psycopg2.connect(
		dbname = config["dbname"],
		user = config["user"],
		password = config["password"],
		host = config["host"],
		port = config["port"]
	)

	print("Connection is set")

	return connection

#close - method that close cusros and connection objects
def close(cursor, connection):
	if cursor:
		cursor.close()
	if connection:
		connection.close()

	print("Cursor and connection have closed")

#xlsx_2_sql. Library OPENPYXL is used
def xlsx_2_sql(path, table, schema, config):
	connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
	connection = create_engine(connect_str)
	excel_data = pd.read_excel(path)
	excel_data.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False)
	#data = pd.DataFrame(excel_data)
	#data.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False)

def csv_2_sql(path, table, schema, config):
	connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
	connection = create_engine(connect_str)
	csv_data = pd.read_csv(path, sep = ';')
	csv_data.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False)