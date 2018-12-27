from coralinedb import MySQLDB
import pandas as pd
from sqlalchemy.types import DECIMAL

host = ''
username = ''
password = ''
db_name = ''


# Initial object
db = MySQLDB(host, username, password)

# Print all databases
print(db.get_databases())

# Print all tables
print(db.get_tables(db_name))

df = pd.DataFrame()
df['A'] = [1.34434, 2.4, 3.3]
df['B'] = [5, 6, 7]
df['C'] = [6, 7, 8]

db.save_table(df, db_name, 'test_table', dtype={'A': DECIMAL(10, 5)})

# Load a table as dataframe
df = db.load_table(db_name, 'test_table')

print(df)

# Execute an sql statement
res = db.execute('drop table test_table', db_name)
