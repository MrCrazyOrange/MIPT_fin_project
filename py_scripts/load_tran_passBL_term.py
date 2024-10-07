import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.types import Numeric
from sqlalchemy.types import DECIMAL
from sqlalchemy.types import Float

# for increment load in terminal_load func
####################
"""
[+] create_new_rows
		создает таблицу tmp_new_rows с новыми записями

[+] create_updated_rows
	создает таблицу tmp_updated_rows с измененными записями

[+] create_deleted_rows
		создает таблицу tmp_deleted_rows с удаленными записями
"""

def create_new_rows(cursor, connection):
	cursor.execute("""
		CREATE TABLE stg_new_rows AS
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM stg_terminals t1
			LEFT JOIN v_terminals t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id IS NULL;
	""")

	connection.commit()

def create_deleted_rows(cursor, connection):
	cursor.execute("""
		CREATE TABLE stg_deleted_rows AS
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM v_terminals t1
			LEFT JOIN stg_terminals t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id IS NULL;
	""")

	connection.commit() 

def create_updated_rows(cursor, connection):
	cursor.execute("""
		CREATE TABLE stg_updated_rows AS
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM v_terminals t1
			INNER JOIN stg_terminals t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t1.terminal_type != t2.terminal_type
				OR t1.terminal_city != t2.terminal_city
				OR t1.terminal_address != t2.terminal_address;
	""")

	connection.commit()

def remove_stg_tables(cursor, connection):
	cursor.execute("""
		SELECT
			table_name
		FROM information_schema.tables
		WHERE table_schema = 'bank'
			AND table_name LIKE 'stg_%';
	""")

	for table in cursor.fetchall():
		cursor.execute(f"DROP TABLE IF EXISTS {table[0]};")

	connection.commit()

def update_terminals_hist(cursor, connection):
	cursor.execute("""
		INSERT INTO DWH_DIM_terminals_hist(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		) SELECT
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM stg_new_rows;	
	""")

	cursor.execute("""
		UPDATE DWH_DIM_terminals_hist
		SET end_dttm = now() - interval '1 second'
		WHERE terminal_id IN (
			SELECT
				terminal_id
			FROM stg_updated_rows
		) AND end_dttm = '2999-12-31 23:59:59';
	""")

	cursor.execute("""
		INSERT INTO DWH_DIM_terminals_hist(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		) SELECT 
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM stg_updated_rows;
	""")

	connection.commit()

def deleted_terminals_hist(cursor, connection):
	cursor.execute("""
		UPDATE DWH_DIM_terminals_hist
		SET end_dttm = now() - interval '1 second'
		WHERE terminal_id IN (
			SELECT
				terminal_id
			FROM stg_deleted_rows
		)
		AND end_dttm = '2999-12-31 23:59:59';
	""")

	cursor.execute("""
		INSERT INTO DWH_DIM_terminals_hist(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			deleted_flg
		) SELECT
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			1
		FROM stg_deleted_rows;
	""")

	connection.commit()
####################
# excel, scv to sql

def excel2sql(path, table, schema, config):
	connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
	connection = create_engine(connect_str)
	excel_data = pd.read_excel(path)
	excel_data.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False)

def excel2sql_terminals(path, table, schema, config):
	connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
	connection = create_engine(connect_str)
	excel_data = pd.read_excel(path)
	excel_data.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False,
		dtype = {"terminal_id": VARCHAR(128),
		"terminal_type": VARCHAR(128),
		"terminal_city": VARCHAR(128),
		"terminal_address": VARCHAR(128)})

#only for transaction table
def csv2sql(path, table, schema, config):
	connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
	connection = create_engine(connect_str)
	df = pd.read_csv(path, sep = ';')
	df.to_sql(name = table, con = connection, schema = schema, if_exists = "replace", index = False,
		dtype = {"transaction_id": VARCHAR(),
				 "transaction_date": TIMESTAMP(),
				 # "amount": Float(),
				 "card_num": VARCHAR(),
				 "oper_type": VARCHAR(),
				 "oper_result": VARCHAR(),
				 "terminal": VARCHAR()})

####################

def transactions_load(cursor, connection, path, schema, config):
	csv2sql(path, "stg_transactions", schema, config)

	cursor.execute("""
		INSERT INTO DWH_FACT_transactions (
			transaction_id,
			transaction_date,
			amount,
			card_num,
			oper_type,
			oper_result,
			terminal
		) SELECT
			transaction_id,
			transaction_date,
			amount,
			card_num,
			oper_type,
			oper_result,
			terminal
		FROM stg_transactions;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS stg_transactions;
	""")

	connection.commit()

def passportBL_load(cursor, connection, path, schema, config):
	excel2sql(path, "stg_passport_blacklist", schema, config)

	cursor.execute("""
		INSERT INTO DWH_FACT_passport_blacklist(
			date_bl,
			passport_num
		) SELECT
			date,
			passport
		FROM stg_passport_blacklist
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS stg_passport_blacklist
	""")

	connection.commit()

def terminal_load(cursor, connection, path, schema, config):
	excel2sql_terminals(path, "stg_terminals", schema, config)

	create_new_rows(cursor, connection)
	create_deleted_rows(cursor, connection)
	create_updated_rows(cursor, connection)
	update_terminals_hist(cursor, connection)
	deleted_terminals_hist(cursor, connection)

	remove_stg_tables(cursor, connection)

	connection.commit()