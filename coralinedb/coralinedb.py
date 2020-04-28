"""
    Coraline DB Manager - This will take care of reading and saving tables to SQL database
"""

# import python packages
import pandas as pd
from sqlalchemy import create_engine
import time


class BaseDB:
    """
    Base class for all DB
    These functions must be inherited by sub-class
        - create_connection
        - show_databases
        - show_tables
    """
    host = ""
    username = ""
    passwd = ""
    port = None
    engines = {}

    def __init__(
        self, 
        host: str, 
        username: str, 
        passwd: str, 
        port: str = None):
        """Initial object by specify host username and password for database connection

        Parameters
        ----------
        host : str
            host url
        username : str
            username of database
        passwd : str
            password of database
        port : str, optional
            port number, by default None
        """
        self.host = host
        self.username = username
        self.passwd = passwd
        self.port = port
        self.engines = {}

    def __del__(self):
        """
        On object deleted
        """
        for en_key in self.engines:
            engine = self.engines[en_key]
            try:
                engine.dispose()
            except :
                # engine cannot be dispose #TODO fix it!!
                pass
    
    
    ################### VIRTUAL METHODS #######################
    def get_engine_url(self, db_name: str):
        """Get Engine URL. This will depend on database, so this function must be overriden by subclass

        Parameters
        ----------
        db_name : str
            database name

        Raises
        ------
        NotImplementedError
            this function must be overriden
        """
        raise NotImplementedError()


    def get_databases(self):
        """list of all accessable databases on this host

        Raises
        ------
        NotImplementedError
            this function must be overriden
        """
        raise NotImplementedError()


    def get_tables(self, db_name: str):
        """List all tables in database

        Parameters
        ----------
        db_name : str
            database name

        Raises
        ------
        NotImplementedError
            this function must be overriden
        """
        raise NotImplementedError()
    #########################################################

    def get_engine(
        self, 
        db_name: str = "", 
        engine_url: str = ""):
        """Create Engine by db_name or engine_url

        Parameters
        ----------
        db_name : str, optional
            database name, by default ""
        engine_url : str, optional
            customize engine url, by default ""

        Returns
        -------
        engine
            created engine
        """

        engine_key = db_name if db_name != "" else "_"

        if engine_key in self.engines:
            engine = self.engines[engine_key]
            try:
                engine.dispose()
            except:
                pass
        
        if engine_url == "":
            engine_url = self.get_engine_url(db_name)

        # Create a new one
        self.engines[engine_key] = create_engine(engine_url)

        return self.engines[engine_key]
        

    def create_connection(
        self, 
        db_name: str = None, 
        raw: bool = False,
        engine_url: str = ""):
        """Create Connection and engine for database

        Parameters
        ----------
        db_name : str, optional
            database name, by default None
        raw : bool, optional
            enable raw connection, by default False
        engine_url : str, optional
            custom engine url, by default ""

        Returns
        -------
        engine
            created engine
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
                connection = engine.connect() if raw == False else engine.raw_connection()

                connected = True

                return engine, connection
            except Exception as e:
                print("Database Connection Error: {}".format(e))
                print("Network is unreachable. Retrying to connect to database in 10 seconds...")
                time.sleep(10)
                max_tries -= 1


    def load_table(
        self, 
        db_name: str, 
        table_name: str, 
        **kwargs) -> pd.DataFrame:
        """Load a table from database

        Parameters
        ----------
        db_name : str
            database name
        table_name : str
            table name

        Returns
        -------
        pd.DataFrame
            loaded table
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Check if table exists and read
        if engine.dialect.has_table(engine, table_name):
            # Prevent duplicate keys
            kwargs.pop("sql", None)
            kwargs.pop("con", None)
            kwargs.pop("coerce_float", None)
            result = pd.read_sql(sql=table_name, con=connection, coerce_float=True, **kwargs)
        else:
            print(table_name, "does not exist")
            result = None

        # Close connection
        connection.close()

        return result

    def load_tables(
        self, 
        db_name: str, 
        table_names: list, 
        **kwargs) -> list:
        """Load all tables from  a database

        Parameters
        ----------
        db_name : str
            database name
        table_names : list
            list of table names

        Returns
        -------
        list
            list of loaded table
        """
        
        # Create Connection
        engine, connection = self.create_connection(db_name)

        dfs = []

        # Prevent duplicate keys
        kwargs.pop("sql", None)
        kwargs.pop("con", None)
        kwargs.pop("coerce_float", None)

        # Load each table
        for tbn in table_names:
            if engine.dialect.has_table(engine, tbn):
                df = pd.read_sql(sql=tbn, con=connection, coerce_float=True, **kwargs)
            else:
                print(tbn, "does not exist")
                df = None
            dfs.append(df)

        # Close connection
        connection.close()

        return dfs

    def save_table(
        self, 
        df: pd.DataFrame, 
        db_name: str,
        table_name: str, 
        index: bool = False, 
        if_exists: str = 'replace', 
        **kwargs):
        """Save pandas dataframe to database

        Parameters
        ----------
        df : pd.DataFrame
            dataframe to be save
        db_name : str
            database name
        table_name : str
            table name
        index : bool, optional
            Write DataFrame index as a column, by default False
        if_exists : str, optional
            How to behave if the table already exists ({‘fail’, ‘replace’, ‘append’}), by default 'replace'
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

    def query(
        self, 
        sql_statement: str, 
        db_name: str = None,
        **kwargs) -> pd.DataFrame:
        """Run SQL query

        Parameters
        ----------
        sql_statement : str
            SQL statement
        db_name : str, optional
            database name, by default None
        **kwargs: see pandas.read_sql() doc

        Returns
        -------
        pd.DataFrame
            data
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


    def get_count(
        self, 
        db_name: str, 
        table_name: str) -> int:
        """Get number of rows of a table

        Parameters
        ----------
        db_name : str
            database name
        table_name : str
            table name

        Returns
        -------
        int
            number of rows
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

    def execute(
        self, 
        sql_statement: str,
        db_name: str = None,
        **kwargs):
        """Execute SQL Statement to database

        Parameters
        ----------
        sql_statement : str
            SQL statement
        db_name : str, optional
            database name, by default None

        Returns
        -------
        object
            metadata of query execution
        """
        
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Execute SQL
        result = connection.execute(sql_statement, **kwargs)

        # Close connection
        connection.close()

        # return metadata of query execution result
        return result


    def call_procedure(
        self, 
        sql_statement: str, 
        db_name: str = None, 
        return_df: bool = False, 
        **kwargs):
        """Execute SQL Stored Procedure Statement to database

        Parameters
        ----------
        sql_statement : str
            SQL statement
        db_name : str, optional
            database name, by default None
        return_df : bool, optional
            return dataframe flag, by default False

        Returns
        -------
        int or pd.DataFrame
            Number of affected rows or pandas dataframe if the corresponding table exists.
        """ 	
        
        # Create Connection
        engine, connection = self.create_connection(db_name, raw=True)

        # Execute Procedure
        cursor = connection.cursor()
        affected_rows = cursor.execute(sql_statement, **kwargs)

        # Get Data
        if return_df == True:
            data = list(cursor.fetchall())
            column_names = [col[0] for col in cursor.description] if cursor.description is not None else None

        # Close connection
        cursor.close()
        connection.commit()
        connection.close()
        
        # return result
        if return_df == True:
            return pd.DataFrame(data, columns = column_names) if column_names is not None else None
        else:
            return affected_rows


def print_help():
    """
    print help
    :return:
    """
    print("Please go to https://pypi.org/project/coralinedb/ to see how to use the package")

