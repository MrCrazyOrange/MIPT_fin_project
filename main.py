from py_scripts.database import *
from py_scripts.table_creation import *
from py_scripts.load_tran_passBL_term import *
from py_scripts.fraud_check import *


####################

config = get_config("db_config.json")
connection = get_connection(config)
cursor = connection.cursor()

cursor.execute("""
	SET search_path TO bank;
""")

create_DMW_tables(cursor, connection)
passportBL_load(cursor, connection, "./data/passport_blacklist_01032021.xlsx", "bank", config)
transactions_load(cursor, connection, "./data/transactions_01032021.txt", "bank", config)
terminal_load(cursor, connection, "./data/terminals_01032021.xlsx", "bank", config)
passport_check(cursor, connection, '2021-03-03 23:59:59')
account_check(cursor, connection, '2021-03-03 23:59:59')
city_check(cursor, connection)
sum_check(cursor, connection)

passportBL_load(cursor, connection, "./data/passport_blacklist_02032021.xlsx", "bank", config)
transactions_load(cursor, connection, "./data/transactions_02032021.txt", "bank", config)
terminal_load(cursor, connection, "./data/terminals_02032021.xlsx", "bank", config)
passport_check(cursor, connection, '2021-03-03 23:59:59')
account_check(cursor, connection, '2021-03-03 23:59:59')
city_check(cursor, connection)
sum_check(cursor, connection)

passportBL_load(cursor, connection, "./data/passport_blacklist_03032021.xlsx", "bank", config)
transactions_load(cursor, connection, "./data/transactions_03032021.txt", "bank", config)
terminal_load(cursor, connection, "./data/terminals_03032021.xlsx", "bank", config)
passport_check(cursor, connection, '2021-03-03 23:59:59')
account_check(cursor, connection, '2021-03-03 23:59:59')
city_check(cursor, connection)
sum_check(cursor, connection)
####################

close(cursor, connection)