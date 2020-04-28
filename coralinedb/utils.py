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
    arr_header = re.sub('[^{}_A-Za-z0-9 ]+'.format(delimiter), '', temp_string).replace(' ', '_').split(delimiter)

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
        dataframe that all datatypes are converted (df)
    """
    for c in df.columns:
        # Convert column to string
        col_data = df[c].map(str)
        col_data = col_data.replace("NaT", None)
        col_data = col_data.replace("NaN", None)

        # Check DATETIME
        try:
            # Check if it's able to convert column to datetime

            # if column is datetime, then skip to convert
            if 'datetime' in str(col_data.dtype):
                continue

            df[c] = pd.to_datetime(col_data)
            continue
        except ValueError:
            pass

        # Check NUMERIC
        try:
            # Drop NaN rows
            series = df[c].dropna()

            # if column_name is int or float, then skip to convert
            if 'int' in str(col_data.dtype) or 'float' in str(col_data.dtype):
                continue

            # Check if it can be converted to numeric
            df[c] = pd.to_numeric(series)

        except ValueError:
            pass

    return df

def get_max_length_columns(df):
    """
    find maximum length of value in each column and ceil it
    :param df: dataframe (df)
    :return:
        array of length of each column, array's length should be equal to number of columns (array)
        array of maximum decimal for float, double, and decimal datatype, otherwise its value is zero
    """

    measurer = np.vectorize(len)
    arr_max_len_columns = []
    arr_max_decimal = []

    for i, x in enumerate(measurer(df.values.astype(str)).max(axis=0)):

        if 'float' in str(df.iloc[:, i].dtype):
            col_data = df.iloc[:, i].map(str).str.extract('\.(.*)')
            max_decimal = measurer(col_data.values.astype(str)).max(axis=0)[0]
            arr_max_decimal.append(max_decimal)
        else:
            arr_max_decimal.append(0)

        arr_max_len_columns.append(ceil(x / 10) * 10)

    return arr_max_len_columns, arr_max_decimal

def convert_df_datatype_to_sqlalchemy_datatype(df):
    """
    convert dataframe's data type into SQLAlchemy's data type
    :param df: dataframe (df)
    which can get this array from get_max_length_columns()
    :return:
        dict of data type of each column in SQLAlchemy standard (dict)
    """
    
    arr_max_len_columns, arr_max_decimal = get_max_length_columns(df)

    dtype_dict = {}

    for i, col_name in enumerate(df.columns):
        if 'bool' in str(df[col_name].dtype):
            # Compatible with SQL-Server and MySQL, since MySQL doesn't have BOOLEAN.
            dtype_dict[col_name] = sqlalchemy.types.INTEGER()
        elif 'int' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.INTEGER()
        elif 'float' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.DECIMAL(precision=arr_max_len_columns[i], scale=arr_max_decimal[i])
        elif 'datetime' in str(df[col_name].dtype):
            dtype_dict[col_name] = sqlalchemy.types.DateTime()
        elif 'object' in str(df[col_name].dtype):
            # check the limit of varhcar, if the length exeeds, then use TEXT
            if arr_max_len_columns[i] > 1000:
                dtype_dict[col_name] = sqlalchemy.types.Text()
            else:
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

    df = get_detected_column_types(df)

    dtype_dict = convert_df_datatype_to_sqlalchemy_datatype(df)

    del df

    return dtype_dict