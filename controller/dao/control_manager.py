from datetime import datetime
from log_manager import LogManager


class ControlLogManager(LogManager):
    def __init__(self):
        super(ControlLogManager, self).__init__()

    def get_log_exists_today(self, store_id):
        """
        Lấy log của ngày hiện tại
        :param store_id: ID của cửa hàng tham chiếu từ table control_file

        :return: Trả về dòng dữ liệu có config_file_id = store_id   
        """
        today = datetime.now().date()  # Lấy ngày hiện tại
        query = f"""
        SELECT *
        FROM log_file
        WHERE config_file_id = %s
        AND DATE(created_at) = %s
        """

        return self.db_connection.execute_query(query, (store_id, today), function_name='get_log_exists_today')

    def create_log(self, store_id, status='PENDING'):
        """
        Tạo bản ghi log mới cho tệp với trạng thái ban đầu là 'PENDING'
        :param store_id: ID của cửa hàng tham chiếu từ table control_file

        :return: Trả về ID của bản ghi log mới tạo
        """

        query = """
        INSERT INTO log_file (config_file_id, status)
        VALUES (%s, %s)
        """

        # Trả về ID của bản ghi log mới tạo
        return self.db_connection.execute_query(query, (store_id, status), function_name='create_log')

    def update_log_status(self, index_id, store_id, status):
        """
        Cập nhật trạng thái log
        :param index_id: ID của bản ghi log
        :param store_id: ID của cửa hàng tham chiếu từ table control_file

        :return status: Trả về số dòng thay đổi
        """
        query = """
        UPDATE log_file
        SET status = %s
        WHERE config_file_id = %s and index_id = %s
        """

        # Trả về số dòng thay đổi
        return self.db_connection.execute_query(query, (status, store_id, index_id), function_name='update_log_status')

    def get_log_status(self, id, store_id):
        """
        Lấy trạng thái log của store_id
        :param id: ID của bản ghi log
        :param store_id: ID của cửa hàng tham chiếu từ table control_file

        :return: Trạng thái log của store_id        
        """

        query = """
        SELECT status
        FROM log_file
        WHERE config_file_id = %s and index_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """

        # Trả về trạng thái log của store_id
        return self.db_connection.execute_query(query, (id, store_id,), function_name='get_log_status')[0]

    def update_log_file_info(self, log_file_id, store_id, file_name, file_size, count):
        """
        Cập nhật thông tin file vào table log_file (file_name, file_size, count)

        :param log_file_id: ID của bản ghi log
        :param store_id: ID của cửa hàng
        :param file_name: Tên file
        :param file_size: Kích thước file
        :param count: Số lượng dòng dữ liệu có trong file

        :return: Số dòng thay đổi
        """

        query = """
        UPDATE log_file
        SET file_name = %s, file_size = %s, count = %s
        WHERE config_file_id = %s and index_id = %s
        """

        return self.db_connection.execute_query(
            query,
            (log_file_id, store_id, file_name, file_size, count),
            function_name='update_log_file_info'
        )

    def update_row_affected_and_status(self, index_id, affected_row, status):
        """
        Cập nhật số dòng thay đổi và trạng thái log

        :param index_id: ID của bản ghi log
        :param affected_row: Số dòng thay đổi
        :param status: Trạng thái log

        :return  Số dòng thay đổi
        """

        query = """
        UPDATE log_file
        SET row = %s, status = %s
        WHERE index_id = %s
        """

        return self.db_connection.execute_query(query, (affected_row, status, index_id), function_name='update_log_file')
