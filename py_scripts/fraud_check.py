# passport check
def passport_check(cursor, connection, date):
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS stg_passport_fraud AS
			SELECT DISTINCT
				client_id,
				CONCAT_WS(' ', last_name, first_name, patronymic) AS fio,
				passport_num,
				passport_valid_to,
				phone
			FROM dwh_dim_clients
			WHERE (%s > passport_valid_to)
				OR passport_num IN (
					SELECT
						passport_num
					FROM dwh_fact_passport_blacklist
				);
	""", [date])

	cursor.execute("""
		CREATE OR REPLACE VIEW std_account_fraud AS
			SELECT DISTINCT
				t1.account,
				t2.fio,
				t2.passport_num,
				t2.phone
			FROM dwh_dim_accounts t1
			INNER JOIN stg_passport_fraud t2
			ON t1.client = t2.client_id
			WHERE t2.passport_valid_to IS NOT NULL;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_cards_fraud AS
			SELECT DISTINCT
				t1.card_num,
				t2.fio,
				t2.passport_num,
				t2.phone
			FROM dwh_dim_cards t1
			INNER JOIN std_account_fraud t2
			ON t1.account = t2.account;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW v_passport_fraud AS
			SELECT DISTINCT
				t2.fio,
				t2.passport_num,
				t2.phone,
				t1.transaction_date
			FROM dwh_fact_transactions t1
			INNER JOIN stg_cards_fraud t2
			ON t1.card_num = t2.card_num;
	""")

	cursor.execute("""
		INSERT INTO report_dt(
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		) SELECT DISTINCT
			transaction_date,
			passport_num,
			fio,
			phone,
			'invalid passport',
			current_timestamp
		FROM v_passport_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS v_passport_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_cards_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_account_fraud;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS stg_passport_fraud CASCADE;
	""")

	connection.commit()
####################

# account check
def account_check(cursor, connection, date):
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS stg_account_fraud AS
			SELECT DISTINCT
				t1.account,
				t1.valid_to,
				CONCAT_WS(' ', t2.last_name, t2.first_name, t2.patronymic) AS fio,
				t2.passport_num,
				t2.phone
			FROM dwh_dim_accounts t1
			LEFT JOIN dwh_dim_clients t2
			ON t1.client = t2.client_id
			WHERE %s > t1.valid_to;
	""", [date])

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_cards_fraud AS
			SELECT DISTINCT
				t1.card_num,
				t2.passport_num,
				t2.fio,
				t2.phone
			FROM dwh_dim_cards t1
			INNER JOIN stg_account_fraud t2
			ON t1.account = t2.account;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW v_passport_fraud AS
			SELECT DISTINCT
				t2.fio,
				t2.passport_num,
				t2.phone,
				t1.transaction_date
			FROM dwh_fact_transactions t1
			INNER JOIN stg_cards_fraud t2
			ON t1.card_num = t2.card_num;
	""")

	cursor.execute("""
			INSERT INTO report_dt(
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			) SELECT DISTINCT
				transaction_date,
				passport_num,
				fio,
				phone,
				'invalid account',
				current_timestamp
			FROM v_passport_fraud;
		""")

	cursor.execute("""
		DROP VIEW IF EXISTS v_passport_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_cards_fraud;
	""")

	cursor.execute("""
		DROP TABLE IF EXISTS stg_account_fraud;
	""")

	connection.commit()
####################

# city check
def city_check(cursor, connection):
	cursor.execute("""
		CREATE OR REPLACE VIEW stg_auth_cities_num AS
			SELECT
				card_num,
				COUNT(DISTINCT t2.terminal_city) AS author_cities
			FROM dwh_fact_transactions t1
			LEFT JOIN dwh_dim_terminals_hist t2
			ON t1.terminal = t2.terminal_id
			GROUP BY t1.card_num
			HAVING COUNT(DISTINCT t2.terminal_city) > 1;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_tran_each_city AS
			SELECT DISTINCT
				t2.card_num,
				t1.transaction_date,
				t3.terminal_city
			FROM dwh_fact_transactions t1
			INNER JOIN stg_auth_cities_num t2
			ON t1.card_num = t2.card_num
			LEFT JOIN dwh_dim_terminals_hist t3
			ON t1.terminal = t3.terminal_id;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_tran_fl_cities AS
			SELECT
				card_num,
				terminal_city AS first_tran_city,
				transaction_date AS first_tran_date,
				LAG(terminal_city) OVER (PARTITION BY card_num ORDER BY transaction_date) AS next_tran_city,
				LAG(transaction_date) OVER (PARTITION BY card_num ORDER BY transaction_date) AS next_tran_date
			FROM stg_tran_each_city;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_tran_timeInter AS
			SELECT
				card_num,
				first_tran_city,
				first_tran_date,
				next_tran_city,
				next_tran_date,
				next_tran_date - first_tran_date AS tran_interval
			FROM stg_tran_fl_cities
			WHERE first_tran_city != next_tran_city
			 AND (next_tran_date - first_tran_date < '1 hour'::INTERVAL
			 	OR next_tran_date - first_tran_date < -'1 hour'::INTERVAL
			 );
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_city_fraud AS
		SELECT
			t1.first_tran_date AS event_dt,
			t4.passport_num AS passport,
			CONCAT_WS(' ', t4.first_name, t4.last_name, t4.patronymic) AS fio,
			t4.phone,
			'suspicious location' AS event_type,
			current_timestamp AS report_dt
		FROM stg_tran_timeInter t1
		LEFT JOIN dwh_dim_cards t2
		ON t1.card_num = t2.card_num
		LEFT JOIN dwh_dim_accounts t3
		ON t2.account = t3.account
		LEFT JOIN dwh_dim_clients t4
		ON t3.client = t4.client_id;
	""")

	cursor.execute("""
		INSERT INTO report_dt (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		FROM stg_city_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_city_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_tran_timeInter;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_tran_fl_cities;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_tran_each_city;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_auth_cities_num CASCADE;
	""")

	connection.commit()
####################

# checking sum
def sum_check(cursor, connection):
	# checking results of last three operations
	cursor.execute("""
		CREATE OR REPLACE VIEW stg_success_op AS
			SELECT
				card_num,
				transaction_date AS current_tran_time,
				LAG(transaction_date, 3) OVER (PARTITION BY card_num ORDER BY transaction_date) AS plus3_tran_time,
				oper_type,
				oper_result AS current_result,
				LAG(oper_result) OVER (PARTITION BY card_num ORDER BY transaction_date) AS result_plus1,
				LAG(oper_result, 2) OVER (PARTITION BY card_num ORDER BY transaction_date) AS result_plus2,
				LAG(oper_result, 3) OVER (PARTITION BY card_num ORDER BY transaction_date) AS result_plus3
			FROM dwh_fact_transactions;
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW stg_sum_fraud AS
			SELECT
				card_num,
				current_tran_time,
				plus3_tran_time,
				current_tran_time - plus3_tran_time AS tran_time_period,
				current_result,
				result_plus1,
				result_plus2,
				result_plus3
			FROM stg_success_op
			WHERE (((current_tran_time - plus3_tran_time) < (SELECT '30 minute'::INTERVAL))
			 	OR ((plus3_tran_time - current_tran_time) < -(SELECT '30 minute'::INTERVAL)))
				AND result_plus1 = 'REJECT'
				AND result_plus2 = 'REJECT'
				AND result_plus3 = 'REJECT';
	""")

	cursor.execute("""
		CREATE OR REPLACE VIEW std_tran_fraud AS
			SELECT
				t1.current_tran_time AS event_dt,
				t4.passport_num AS passport,
				CONCAT_WS(' ', t4.first_name, t4.last_name, t4.patronymic) AS fio,
				t4.phone,
				'suspicious sum' AS event_type,
				current_timestamp AS report_dt
			FROM stg_sum_fraud t1
			LEFT JOIN dwh_dim_cards t2
			ON t1.card_num = t2.card_num
			LEFT JOIN dwh_dim_accounts t3
			ON t2.account = t3.account
			LEFT JOIN dwh_dim_clients t4
			ON t3.client = t4.client_id;
	""")

	cursor.execute("""
		INSERT INTO report_dt (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		FROM std_tran_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS std_tran_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_sum_fraud;
	""")

	cursor.execute("""
		DROP VIEW IF EXISTS stg_success_op CASCADE;
	""")

	connection.commit()