import mysql.connector as connector
from dotenv import load_dotenv
import os


class Database:
    def __init__(self, database_name: str):
        self.database_name = database_name
        self.db = self.connect(self.database_name)

    def connect(self, database: str):
        HOST = os.getenv('HOST')
        USERNAME = os.getenv('USERNAME')
        PASSWORD = os.getenv('PASSWORD')

        db = connector.connect(
            host=HOST,
            user=USERNAME,
            password=PASSWORD,
            database=database
        )
        # check connection is established
        if db.is_connected():
            print("Connected to MySQL database")
            return db
        else:
            print("Failed to connect to MySQL database")
            return None

    def close(self):
        self.db.close()
        print("MySQL database connection closed")
