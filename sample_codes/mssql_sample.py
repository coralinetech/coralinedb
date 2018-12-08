from coralinedb import MSSQLDB
import pandas as pd

host = ""
username = ''
password = ''
db_name = ''

# Initial object
db = MSSQLDB(host, username, password)

# Print all databases
print(db.get_databases())

# Print all tables
print(db.get_tables(db_name))

# Load a table as dataframe
df = db.load_table(db_name, 'branch')
print(df)

df = pd.DataFrame()
df['A'] = [1, 2, 3]
df['B'] = [5, 6, 7]
df['C'] = [6, 7, 8]

# Save to a table
db.save_table(df, db_name, 'test_table')

# Execute an sql statement
res = db.execute('drop table test_table', db_name)
