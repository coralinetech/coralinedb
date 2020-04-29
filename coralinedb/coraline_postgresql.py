# import python packages
import pandas as pd
from coralinedb import BaseDB


class PostgreSQLDB(BaseDB):
    """
    Class for PostgreSQL Database
    """
    def get_engine_url(self, db_name: str) -> str:
        """get engine URL

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
            self.port = '5432'

        return f"postgresql://{self.username}:{self.passwd}@{self.host}:{self.port}/{db_name}"


    def load_table(self, db_name: str, table_name: str) -> pd.DataFrame:
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
        _, connection = self.create_connection(db_name)

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