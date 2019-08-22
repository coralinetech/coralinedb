# import python packages
import pandas as pd
from sqlalchemy import create_engine
from coralinedb import BaseDB


class PostgreSQLDB(BaseDB):
    """
    Class for PostgreSQL Database
    """

    def get_engine(self, db_name):
        """
        Get engine by db_name
        :return: DB engine
        """
        engine_key = db_name if db_name != "" else "_"

        if engine_key in self.engines:
            engine = self.engines[engine_key]
            try:
                engine.dispose()
            except:
                pass

        # Create a new one
        self.engines[engine_key] = create_engine("postgresql://" + self.username + ":" + self.passwd + '@' + self.host + ':5432/' + db_name)

        return self.engines[engine_key]

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
        try:
            sql = 'SELECT * FROM %s' % table_name
            result = pd.read_sql(sql, connection, coerce_float=True)
        except Exception as e:
            print(e)
            result = None

        # Close connection
        connection.close()

        return result

    # def get_databases(self):
    #     """
    #     list of all accessable databases on this host
    #     :return: list of database names
    #     """
    #
    #     # Create Connection
    #     engine, connection = self.create_connection()
    #
    #     # Get result
    #     sql = 'show databases;'
    #     result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values
    #
    #     # Close Connection
    #     connection.close()
    #
    #     return result
    #
    # def get_tables(self, db_name):
    #     """
    #     List all tables in database
    #     :param db_name:  database name (str)
    #     :return: list of table names
    #     """
    #     # Create Connection
    #     engine, connection = self.create_connection(db_name)
    #
    #     sql = 'show tables;'
    #     result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values
    #
    #     # Close Connection
    #     connection.close()
    #
    #     return result

