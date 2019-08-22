from coralinedb import PostgreSQLDB
import pandas as pd
from sqlalchemy.types import DECIMAL

host = ''
username = ''
password = ''
db_name = ''
schema = ''


# Initial object
db = PostgreSQLDB(host, username, password)

# # Print all databases
# print(db.get_databases(db_name))

# # Print all tables
# print(db.get_tables(db_name))

df = pd.DataFrame()
df['A'] = [1.34434, 2.4, 3.3]
df['B'] = [5, 6, 7]
df['C'] = [6, 7, 8]

db.save_table(df, db_name, 'test_table', schema=schema, dtype={'A': DECIMAL(10, 5)})

# Load a table as dataframe
df = db.load_table(db_name, f'{schema}.test_table')

# Execute an sql statement
res = db.execute(f'drop table {schema}.test_table', db_name)
