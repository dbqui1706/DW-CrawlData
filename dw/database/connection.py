import mysql.connector as connector
from dotenv import load_dotenv
import os
from utils.utils import send_email


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
        password = os.getenv('PASSWORD')

        db = connector.connect(
            host=host,
            user=username,
            password=password,
            allow_local_infile=True
        )
        # check connection is established
        if db.is_connected:
            print(f"MySQL database connected")
            return db
        else:
            print(f"MySQL database connection failed")
            send_email(email=os.getenv('EMAIL'), config_id=-1, stage='connection',
                       error_message='MySQL database connection failed')
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
