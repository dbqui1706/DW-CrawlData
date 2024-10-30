from connector import *
from data_validator import DataValidator
from controller.dao.control_manager import ControlLogManager
from controller.dao.staging_manager import StaingManager
from file_manager import FileManager
from data_transfer_manager import DataTransferManager


class DataProcessor:
    def __init__(self):
        pass


class DataProcessorStaging:
    def __init__(self):
        # 1. Kết nối đến DB CONTROL_LAPTOP
        self.control_db = DBControl()
        self.staging_db = DBStaging()
        self.control_logger = ControlLogManager(self.control_db)
        self.control_db_staging = StaingManager(self.staging_db)
        # self.data_validator = DataValidator(self.staging_db)
        self.file_manager = FileManager()
        self.transfer_manager = DataTransferManager(self.control_db)

    def process_data(self):
        # 2. Lấy các cửa hàng từ bảng config_file
        data_stores = self.control_db.get_data_from_config_file()
        # 3. Xử lý dữ liệu cho từng cửa hàng
        for store in data_stores:
            store_id = store['index_id']
            source_folder_location = store['source_folder_location'].strip()
            print(f"Source folder location: {source_folder_location}")
            # 3.1 Lấy file mới nhất trong ngày từ trường source_folder_location
            file_path = self.file_manager.get_latest_file(
                source_folder_location)

            # 3.2 Nếu file mới không tồn tại thì bỏ qua
            if file_path is None:
                continue
            # 3.3 Kiểm tra log đã tồn tại cho config_file trong ngày chưa
            log_exists = self.control_logger.get_log_exists_today(store_id)
            if not log_exists:
                # Nếu chưa, tạo log mới với trạng thái PENDING
                log_exists = self.control_logger.create_log(store_id)

            # 3.4 Kiểm tra STATUS của log
            log_file_id = log_exists['index_id']
            log_status = self.control_logger.get_log_status(
                log_file_id, store_id)

            if log_status != 'PENDING':
                # Nếu trạng thái không phải PENDING, quay lại bước 2.1 để kiểm tra file mới
                continue

            # 3.5 Thay đổi trạng thái thành IN_PROGRESS khi bắt đầu quá trình
            self.control_logger.update_log_status(
                log_file_id, store_id, 'IN_PROGRESS')

            # 3.6 Cập nhập thông tin file vào table log_file (file_name, file_size, count)
            file_name, file_size, count = self.file_manager.get_file_path_info(
                file_path)
            self.control_logger.update_log_file_info(
                log_file_id, store_id, file_name, file_size, count)

            # 3.7 Tiến hành insert dữ liệu từ file vào DB CONTROL_LAPTOP
            row_affected = self.control_db_staging.insert_data_from_file(
                file_path)
            print(
                f"INSERTED: {row_affected['inserted']}, UPDATED: {row_affected['updated']}")
            # 3.8 Tiến hành update số dòng ảnh hưởng và STATUS của log trong DB CONTROLL LAPTOP
            self.control_logger.update_row_affected_and_status(
                log_file_id, row_affected, 'COMPLETED')


if __name__ == "__main__":
    # import shutil
    # import os
    # import datetime

    # def copy_file_with_current_timestamp(source_path: str, destination_path: str):
    #     # Bước 1: Sao chép file
    #     # copy2 sẽ giữ nguyên metadata
    #     shutil.copy2(source_path, destination_path)

    #     # Bước 2: Cập nhật thời gian chỉnh sửa file đích theo thời gian hiện tại
    #     current_time = datetime.datetime.now().timestamp()
    #     os.utime(destination_path, (current_time, current_time))

    #     print(
    #         f"Đã sao chép file và cập nhật thời gian chỉnh sửa: {destination_path}")

    # # Thử nghiệm
    # source_file = "D:\\University\\2024-2025\\kh1_4th\\data warehouse\\data\\phongvu\\phongvu_2024-10-26_11-55-24.csv"
    # destination_file = "D:\\University\\2024-2025\\kh1_4th\\data warehouse\\data\\phongvu\\phongvu_2024-10-28_15-42-59.csv"
    # copy_file_with_current_timestamp(source_file, destination_file)

    processor = DataProcessorStaging()
    processor.process_data()
