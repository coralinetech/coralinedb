from sqlalchemy import create_engine
import pandas as pd
from coralinedb import BaseDB


class MSSQLDB(BaseDB):
    """
    Class for MS SQL Server
    """

    def get_engine(self, db_name):
        """
        Get engine by db_name
        :return: DB engine
        """
        engine_key = db_name if db_name != "" else "_"

        if engine_key not in self.engines:
            # Create a new one
            self.engines[engine_key] = create_engine("mssql+pymssql://" + self.username + ":" + self.passwd + '@' + self.host + '/' + db_name)

        return self.engines[engine_key]

    def get_databases(self):
        """
        list of all accessable databases on this host
        :return: list of database names
        """
        # Create Connection
        engine, connection = self.create_connection()

        # Get results
        sql = 'SELECT name FROM master.sys.databases;'
        result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values

        # Close Connection
        connection.close()

        return result

    def get_tables(self, db_name):
        """
        List all tables in database
        :param db_name:  database name (str)
        :return: list of table names
        """
        # Create Connection
        engine, connection = self.create_connection(db_name)

        # Get result
        sql = 'SELECT * FROM information_schema.tables;'
        result = pd.read_sql(sql, connection, coerce_float=True)['TABLE_NAME'].values

        # Close Connection
        connection.close()

        return result

