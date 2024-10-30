import mysql.connector as connector
from dotenv import load_dotenv
import os


class DBConnection:

    def __init__(self, database_name: str):
        self.database_name = database_name
        self.db = self.get_connection()
        self._cursor = self.db.cursor(dictionary=True)

    def get_connection(self):
        load_dotenv()
        HOST = os.getenv('HOST')
        USERNAME = os.getenv('ACCOUNT')
        PASSWORD = os.getenv('PASSWORD')

        db = connector.connect(
            host=HOST,
            user=USERNAME,
            password=PASSWORD,
            database=self.database_name,
        )
        # check connection is established
        if db.is_connected():
            print(f"Connected to MySQL database {self.database_name}")
            return db
        else:
            print(f"Failed to connect to MySQL database {self.database_name}")
            return None

    def cursor(self):
        return self._cursor

    def close_connection(self):
        self.db.close()
        print(f"MySQL database {self.database_name} connection closed")

    def execute_query(self, query, params=None, function_name=None):
        cursor = self.cursor()  # Tạo cursor
        try:
            if params:
                cursor.execute(query, params)  # Thực hiện lệnh SQL với tham số
            else:
                cursor.execute(query)  # Thực hiện lệnh SQL không có tham số

            # Nếu là SELECT, trả về tất cả kết quả
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()  # Lấy tất cả kết quả
                return results

            self.db_connection.commit()  # Commit thay đổi nếu không phải SELECT
            if query.strip().upper().startswith("INSERT"):
                return cursor.lastrowid  # Trả về ID của bản ghi mới tạo
            elif query.strip().upper().startswith("UPDATE") \
                    or query.strip().upper().startswith("DELETE"):
                # Trả về số bản ghi đã bị ảnh hưởng  # Trả về ID của bản ghi mới tạo (nếu có)
                return cursor.rowcount

        except Exception as e:
            # In ra lỗi
            print(f"An error occurred in function `{function_name}`: {e}")
            self.db_connection.rollback()  # Rollback nếu có lỗi
            return None  # Trả về None để chỉ ra rằng đã có lỗi xảy ra

        finally:
            cursor.close()  # Đóng cursor


class DBControl(DBConnection):
    def __init__(self, database_name: str = 'CONTROL_LAPTOP)'):
        super().__init__(database_name)

    def get_data_from_config_file(self):
        """
        Truy xuất dữ liệu từ bảng config_file.
        """
        try:
            cursor = self.connection.cursor()
            query = "SELECT * FROM config_file"
            cursor.execute(query)
            data = cursor.fetchall()
            return data
        except connector.Error as err:
            print(f"Error retrieving data from config_file: {err}")
            return None
        finally:
            cursor.close()


class DBStaging(DBConnection):
    def __init__(self, database_name: str = 'STAGING_LAPTOP'):
        super().__init__(database_name)
