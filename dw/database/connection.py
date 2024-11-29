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
        self.connection = None
        self.database_name = None

    def get_connection(self):
        """
                Thiết lập kết nối với cơ sở dữ liệu MySQL.
        """
        load_dotenv()
        host = os.getenv('HOST')
        username = os.getenv('ACCOUNT')
        passwork = os.getenv('PASSWORD')
        # db = connector.connect(
        #     host="localhost",
        #     user="root",
        #     password="1234",
        #     allow_local_infile=True
        # )

        try:
            self.connection = connector.connect(
                host=host,
                user=username,
                password=passwork,
                allow_local_infile=True
            )

            if self.connection.is_connected():
                print(f"MySQL database connected")
                return self.connection
            else:
                print(f"MySQL database connection failed")
                return None
        except connector.Error as err:
            print(f"Error connecting to MySQL: {err}")
            return None

    def close_connection(self):
        # self.db.close()
        # print(f"MySQL database {self.database_name} connection closed")
        """
            Đóng kết nối cơ sở dữ liệu.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print(f"MySQL database connection closed")
        else:
            print("No active database connection to close.")