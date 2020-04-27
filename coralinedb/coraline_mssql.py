import pandas as pd
from coralinedb import BaseDB


class MSSQLDB(BaseDB):
    """
    Class for MS SQL Server
    """
    def get_engine_url(self, db_name: str) -> str:
        """Get Engine URL for MS SQL Server

        Parameters
        ----------
        db_name : str
            database name

        Returns
        -------
        str
            engine url
        """
        # Set Default Port
        if self.port is None:
            self.port = '1433'

        return f"mssql+pymssql://{self.username}:{self.passwd}@{self.host}:{self.port}/{db_name}?charset=utf8mb4"


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
        _, connection = self.create_connection(db_name)

        # Get result
        sql = 'SELECT * FROM information_schema.tables;'
        result = pd.read_sql(sql, connection, coerce_float=True)['TABLE_NAME'].values

        # Close Connection
        connection.close()

        return result

