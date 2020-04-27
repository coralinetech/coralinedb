# import python packages
import pandas as pd
from coralinedb import BaseDB
import pymysql
pymysql.install_as_MySQLdb()


class MySQLDB(BaseDB):
    """
    Class for MySQL Database
    """

    def get_engine_url(self, db_name: str) -> str:
        """Get engine URL for MySQL

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
            self.port = '3306'

        return f"mysql://{self.username}:{self.passwd}@{self.host}:{self.port}/{db_name}?charset=utf8mb4"
        

    def get_databases(self):
        """
        list of all accessable databases on this host
        :return: list of database names
        """
        # Create Connection
        engine, connection = self.create_connection()

        # Get result
        sql = 'show databases;'
        result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values

        # Close Connection
        connection.close()

        return result


    def get_tables(self, db_name: str):
        """
        List all tables in database
        :param db_name:  database name (str)
        :return: list of table names
        """
        # Create Connection
        _, connection = self.create_connection(db_name)

        sql = 'show tables;'
        result = pd.read_sql(sql, connection, coerce_float=True).iloc[:, 0].values

        # Close Connection
        connection.close()

        return result

