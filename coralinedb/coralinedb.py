"""
    Coraline DB Manager - This will take care of reading and saving tables to SQL database
"""

# import python packages
import pandas as pd
import time


class BaseDB:
    """
    Base class for all DB
    These functions must be inherited by sub-class
        - create_connection
        - show_databases
        - show_tables
    """
    def __init__(self, host, username, passwd):
        """
        Initial object by specify host username and password for database connection
        :param host: host name of the database (str)
        :param username: username of the database (str)
        :param passwd: password of the database (str)
        """
        self.host = host
        self.username = username
        self.passwd = passwd
        self.engines = {}

    def __del__(self):
        """
        On delete object
        :return:
        """
        for en_key in self.engines:
            engine = self.engines[en_key]
            try:
                engine.dispose()
            except :
                # engine cannot be dispose #TODO fix it!!
                pass

    def get_engine(self, db_name):
        """
        Get engine for db name
        :return:
        """
        pass

    def create_connection(self, db_name=None):
        """
        Create Connection and engine for database
        :param: db_name : name of connecting database (str)
        :return: engine and connection
        """
        connected = False
        max_tries = 10

        # if db_name is not defined, let it be empty string
        if db_name is None:
            db_name = ""

        # Reconnect until max_tries exceeded
        while not connected and max_tries > 0:
            try:
                # create engine from db settings
                engine = self.get_engine(db_name)

                # Create connection for query
                connection = engine.connect()

                connected = True

                return engine, connection
            except Exception as e:
                print("Database Connection Error: {}".format(e))
                print("Network is unreachable. Retrying to connect to database in 10 seconds...")
                time.sleep(10)
                max_tries -= 1

    def try_decoration(self, func):
        """
        Decoration for looping tries
        :return:
        """
        while True:
            try:
                func()
                break
            except:
                print("")

    def load_table(self, db_name, table_name):
        """
        Load a table from database
        *The whole table will be download, please make sure you have enough memory*
        :param db_name: name of database (str)
        :param table_name: table name to be read (str)
        :return: pandas dataframe if table exists. Otherwise, None
        """

        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Check if table exists and read
        if engine.dialect.has_table(engine, table_name):
            sql = 'SELECT * FROM %s' % table_name
            result = pd.read_sql(sql, connection, coerce_float=True)
        else:
            print(table_name, "does not exist")
            result = None

        # Close connection
        connection.close()

        return result

    def load_tables(self, db_name, table_names):
        """
        Load all tables from database
        *The whole table will be download, please make sure you have enough memory*
        :param db_name: name of database (str)
        :param table_names: list of table names (list of strings)
        :return: list of pandas dataframes if the corresponding table exists. Otherwise, None
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        dfs = []

        # Load each table
        for tbn in table_names:
            if engine.dialect.has_table(engine, tbn):
                df = pd.read_sql('SELECT * FROM %s' % tbn, connection, coerce_float=True)
            else:
                print(tbn, "does not exist")
                df = None
            dfs.append(df)

        # Close connection
        connection.close()

        return dfs

    def save_table(self, df, db_name, table_name, index=False, if_exists='replace', **kwargs):
        """
        Save pandas dataframe to database
        :param df: dataframe to be save (pandas dataframe)
        :param db_name: name of database (str)
        :param table_name: name of table (str)
        :param index: Write DataFrame index as a column (boolean)
        :param if_exists: How to behave if the table already exists ({‘fail’, ‘replace’, ‘append’})
        :param  kwargs: pandas to_sql arguments e.g. if_exists, dtype, ...
        :return:
        """

        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Prevent duplicate keys
        kwargs.pop("name", None)
        kwargs.pop("con", None)

        # Write df to database
        df.to_sql(name=table_name, con=engine, index=index, if_exists=if_exists, **kwargs)

        # Close connection
        connection.close()

    def get_databases(self):
        """
        list of all accessable databases on this host
        :return: list of database names
        """
        pass

    def get_tables(self, db_name):
        """
        List all tables in database
        :param db_name:  database name (str)
        :return: list of table names
        """
        pass

    def query(self, sql_statement, db_name=None, **kwargs):
        """
        Run SQL query
        :param sql_statement: SQL statement (str)
        :param db_name: database name
        :param **kwargs: see pandas.read_sql() doc
        :return:
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Prevent duplicate keys
        kwargs.pop("sql", None)
        kwargs.pop("con", None)
        kwargs.pop("coerce_float", None)

        result = pd.read_sql(sql=sql_statement, con=connection, coerce_float=True, **kwargs)

        # Close connection
        connection.close()

        return result

    def get_count(self, db_name, table_name):
        """
        Get number of rows of a table
        :param db_name: database name (str)
        :param table_name: table name (str)
        :return:
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Check if table exists
        if engine.dialect.has_table(engine, table_name):
            sql = 'select count(*) from %s;' % table_name
            result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values[0]
        else:
            print(table_name, "does not exist")
            result = None

        # Close connection
        connection.close()

        return result

    def execute(self, sql_statement, db_name=None):
        """
        Execute SQL Statement to database
        :param sql_statement: sql statement (str)
        :param db_name: database name (str)
        :return:
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Execute SQL
        result = connection.execute(sql_statement)

        # Close connection
        connection.close()

        # return metadata of query execution result
        return result


def print_help():
    """
    print help
    :return:
    """
    print("Please go to https://pypi.org/project/coralinedb/ to see how to use the package")

