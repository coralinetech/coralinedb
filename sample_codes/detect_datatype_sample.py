import os
import sys
import json
import pandas as pd
import coralinedb as cdb

# Go to coralinedb/coralinedb directory
CORALINE_DB_DIR = os.path.abspath(os.path.join(os.getcwd(), os.pardir, 'coralinedb'))
sys.path.append(CORALINE_DB_DIR)

from utils import get_simplified_column_name_and_delimiter, get_datatype_each_col

DB_HOST = ''
DB_USER = ''
DB_PASSWORD = ''
DB_SCHEMA_NAME = ''
DB_TABLE_NAME = ''

if __name__ == "__main__":

    file_path = './dataset.csv'

    # get array of simplified column names
    arr_header, delimiter = get_simplified_column_name_and_delimiter(file_path)

    df = pd.read_csv(file_path, sep=delimiter, header=0)

    # assign custom simplified column names to dataframe
    df.columns = arr_header

    # find data type and maximum data length for each column
    datatype_dict = get_datatype_each_col(df, file_path)

    print(datatype_dict)

    # add parameter dtype, please see PyMySQL for more info
    db = cdb.MySQLDB(DB_HOST, DB_USER, DB_PASSWORD)
    db.save_table(df, DB_SCHEMA_NAME, DB_TABLE_NAME, dtype=datatype_dict)
    