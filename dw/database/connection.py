import mysql.connector

import mysql.connector as connector
from dotenv import load_dotenv
import os


class DBConnection:
    def __init__(self):
        """
        Initialize a new instance of DBConnection class.
        Returns: 
        """

    def get_connection(self):
        load_dotenv()
        HOST = os.getenv('HOST')
        USERNAME = os.getenv('ACCOUNT')
        PASSWORD = os.getenv('PASSWORD')

        db = connector.connect(
            host="localhost",
            user="root",
            password="1234",
            allow_local_infile=True
        )
        # check connection is established
        if db.is_connected:
            print(f"MySQL database connected")
            return db
        else:
            print(f"MySQL database connection failed")
            return None

    def close_connection(self):
        self.db.close()
        print(f"MySQL database {self.database_name} connection closed")
