This is a Coraline Database Manager Package on PyPI. Now, only SQL DB is supported

=== How to use ===

1 - Initial database object by specify host, username and password

from coralinedb import SQLDB
sql_db = SQLDB(host, username, password)



2 - Load a table or tables using

new_table = sql_db.load_table("database_name", "table_name")

or

table1, table2 = sql_db.load_table("database_name", ["table_name1", "table_name2"])



3 - Save dataframe to a table using

sql_db.save_table(df, "database_name", "table_name")



4 - Get number of rows for a table

n_rows = sql_db.get_count("database_name", "table_name")



5 - Run other SQL statement on host (with or without database name)

e.g.
table = sql_db.query("show databases;")
table = sql_db.query("SELECT * FROM ...", "database_name")