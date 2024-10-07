import psycopg2

def create_DMW_tables(cursor, connection):
	cursor.execute("""
		DROP TABLE IF EXISTS DWH_FACT_transactions CASCADE;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS  v_terminals CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS DWH_DIM_terminals_hist CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS DWH_DIM_cards CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS DWH_DIM_accounts CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS DWH_DIM_clients CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS DWH_FACT_passport_blacklist CASCADE;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS report_dt CASCADE;
	""")

	cursor.execute("""
		CREATE TABLE DWH_DIM_clients(
			client_id varchar(128) PRIMARY KEY,
			last_name varchar(128),
			first_name varchar(128),
			patronymic varchar(128),
			date_of_birth date,
			passport_num varchar(128),
			passport_valid_to date,
			phone varchar(128),
			create_dt date,
			update_dt date
		);
	""")

	cursor.execute("""
		INSERT INTO DWH_DIM_clients(
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			create_dt,
			update_dt
		) SELECT
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			create_dt,
			update_dt
		FROM clients;
	""")

	cursor.execute("""
		CREATE TABLE DWH_DIM_accounts(
			account varchar(128) PRIMARY KEY,
			valid_to date,
			client varchar(128) REFERENCES DWH_DIM_clients(client_id),
			create_dt date,
			update_dt date
		);
	""")

	cursor.execute("""
		INSERT INTO DWH_DIM_accounts(
			account,
			valid_to,
			client,
			create_dt,
			update_dt
		) SELECT
			account,
			valid_to,
			client,
			create_dt,
			update_dt
		FROM accounts;
	""")

	cursor.execute("""
		CREATE TABLE DWH_DIM_cards(
			card_num varchar(128) PRIMARY KEY,
			account varchar(128) REFERENCES DWH_DIM_accounts(account),
			create_dt date,
			update_dt date
		); 
	""")

	cursor.execute("""
		INSERT INTO DWH_DIM_cards(
			card_num,
			account,
			create_dt,
			update_dt
		) SELECT
			card_num,
			account,
			create_dt,
			update_dt
		FROM cards;
	""")

	cursor.execute("""
		CREATE TABLE DWH_DIM_terminals_hist(
			id SERIAL PRIMARY KEY,
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			start_dttm TIMESTAMP DEFAULT current_timestamp,
			end_dttm TIMESTAMP DEFAULT '2999-12-31 23:59:59',
			deleted_flg NUMERIC(1) DEFAULT 0
		);
	""")

	cursor.execute(""" 
		CREATE VIEW v_terminals AS
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			FROM DWH_DIM_terminals_hist
			WHERE current_timestamp >= start_dttm
				AND current_timestamp <= end_dttm
	""")

	cursor.execute("""
		CREATE TABLE DWH_FACT_transactions(
			transaction_id varchar(128),
			transaction_date TIMESTAMP,
			amount varchar(128),
			card_num varchar(128) REFERENCES DWH_DIM_cards(card_num),
			oper_type varchar(128),
			oper_result varchar(128),
			terminal varchar(128)
		);
	""")

	cursor.execute("""
		CREATE TABLE DWH_FACT_passport_blacklist(
			date_bl date,
			passport_num varchar(128)
		);
	""")

	# for fio varchar(384) as f i o have 128 for each. 128 * 3 = 384
	cursor.execute("""
		CREATE TABLE report_dt(
			event_dt date,
			passport varchar(128),
			fio varchar(384),
			phone varchar(128),
			event_type varchar(128),
			report_dt date DEFAULT current_timestamp
		);
	""")

	connection.commit()