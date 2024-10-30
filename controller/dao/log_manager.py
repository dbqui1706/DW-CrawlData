from datetime import datetime
from connector import DBConnection
import pandas as pd


class LogManager:
    def __init__(self, db_connection: DBConnection):
        self.db_connection = db_connection

    def create_log_entry(self, config_id, file_name, status):
        # Tạo bản ghi log mới cho tệp với trạng thái ban đầu là 'PENDING'
        pass

    def update_log_status(self, log_id, status, row_count=None):
        # Cập nhật trạng thái log, thêm số lượng dòng đã xử lý
        pass

    def log_error(self, log_id, error_message):
        # Ghi log lỗi cho bản ghi log hiện tại
        pass
