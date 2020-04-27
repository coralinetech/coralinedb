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

