import os
import re
import operator
import sqlalchemy
import pandas as pd
import numpy as np
from math import ceil
from operator import itemgetter

def get_simplified_column_name_and_delimiter(file_path):
    """
    detect delimiter in text file by glancing at first row
    :param file_path: path of text file (str)
    :return:
        number of delimiters that are detected (int)
        delimiter (str)
    """

    first_line = None
    with open(file_path, encoding='utf8') as file:
        first_line = file.readline()

    list_of_count_delimiters = [
        (',', first_line.count(',')),
        ('|', first_line.count('|')),
        ('\t', first_line.count('\t'))
    ]

    delimiter = max(list_of_count_delimiters, key=itemgetter(1))[0]

    arr_header = simplify_column_name(first_line, delimiter)

    return arr_header, delimiter


def simplify_column_name(column_name, delimiter):
    """
    simplify all the columnname by removing all non-alphabetic character 
    except number, and converting name into snake case
    :param column_name: column name (str)
    :param deliemter: delimiter that detected from detect_delimiter() (str)
    :return:
       array of preprocessed column name (array)
    """

    temp_string = column_name.lower()
    arr_header = re.sub('[^{}A-Za-z0-9 ]+'.format(delimiter), '', temp_string).replace(' ', '_').split(delimiter)

    return arr_header

def convert_df_to_datetime(df, column_name):
    """
    Try to convert data type in given column into datetime data type
    :param df: dataframe (df)
    :param column_name: name of column (str)
    :return: 
        processed dataframe (df)
    """

    # if df.column_name is datetime, then skip to convert df to datetime
    if 'datetime' in str(df[column_name].dtype):
        return df
        
    df[column_name] = df[column_name].map(str)
    df[column_name] = df[column_name].replace("NaT", None)
    df[column_name] = df[column_name].replace("NaN", None)
    
    try:
        df[column_name] = pd.to_datetime(df[column_name])
    except ValueError:
        pass
    
    return df

def get_detected_column_types(df):
    """
    Get data type of each columns ('DATETIME', 'NUMERIC' or 'STRING')
    :param df: pandas dataframe
    :return: 
        columns dict (dict)
    """
    col_dict = {}
    for c in df.columns:
        # Convert column to string
        col_data = df[c].map(str)
        col_data = col_data.replace("NaT", None)
        col_data = col_data.replace("NaN", None)

        # Check DATETIME
        try:
            # Check if it's able to convert column to datetime
            pd.to_datetime(col_data)
            col_dict[c] = 'DATETIME'
            continue
        except ValueError:
            pass

        # Check NUMERIC
        try:
            # Drop NaN rows
            series = df[c].dropna()

            # Check if it can be converted to numeric
            pd.to_numeric(series)

            if df[c].dtype == object:
                col_dict[c] = 'STRING'
            else:
                col_dict[c] = 'NUMERIC'
        except ValueError:
            # Otherwise, it's VARCHAR column
            col_dict[c] = "STRING"

    return col_dict

def detect_and_convert_datatype(df):
    """
    detect the data type of each column 
    and convert each column into recommended data type
    :param df: dataframe (df)
    :return:
        new dataframe (df)
    """

    column_dict = get_detected_column_types(df)

    # try to convert datetime according to column_dict
    for x in column_dict:
        if column_dict[x] == 'DATETIME':
            df = convert_df_to_datetime(df, x)

    return df

def get_max_length_columns(df):
    """
    find maximum length of value in each column and ceil it
    :param df: dataframe (df)
    :return:
        array of length of each column, array's length should be equal to number of columns (array)
    """

    measurer = np.vectorize(len)
    arr_max_len_columns = []

    for x in measurer(df.values.astype(str)).max(axis=0):
        arr_max_len_columns.append(ceil(x / 10) * 10)

    return arr_max_len_columns

def convert_df_datatype_to_sqlalchemy_datatype(df):
    """
    convert dataframe's data type into SQLAlchemy's data type
    :param df: dataframe (df)
    which can get this array from get_max_length_columns()
    :return:
        dict of data type of each column in SQLAlchemy standard (dict)
    """
    
    arr_max_len_columns = get_max_length_columns(df)

    dtype_dict = {}

    for i, col_name in enumerate(df.columns):
        if 'bool' in str(df[col_name].dtype):
            # Compatible with SQL-Server and MySQL, since MySQL doesn't have BOOLEAN.
            dtype_dict[col_name] = sqlalchemy.types.INTEGER()
        elif 'int' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.INTEGER()
        elif 'float' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.DECIMAL(precision=arr_max_len_columns[i], scale=4)
        elif 'datetime' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.DateTime()
        elif 'object' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.VARCHAR(length=arr_max_len_columns[i])
        else:
            dtype_dict[col_name] = sqlalchemy.types.VARCHAR(length=arr_max_len_columns[i])

    return dtype_dict


def get_datatype_each_col(df, file_path):
    """
    main function to call sub-function in order to find data type and data length for each column
    :param df: dataframe (df)
    :param file_path: path_to_file (str)
    :return:
        dict of data type of each column in SQLAlchemy standard (dict)
    """

    df = detect_and_convert_datatype(df)

    dtype_dict = convert_df_datatype_to_sqlalchemy_datatype(df)

    del df

    return dtype_dict