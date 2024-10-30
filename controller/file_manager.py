import os
import datetime
import csv


class FileManager:
    @staticmethod
    def get_latest_file(store_location):
        # Logic để tìm file mới nhất trong ngày hiện tại thông qua store_location
        # Lấy ngày hiện tại để kiểm tra file
        today_date = datetime.datetime.now().date()
        latest_file = None
        latest_time = None

        # Kiểm tra tất cả các file trong thư mục
        for file_name in os.listdir(store_location):
            file_path = os.path.join(store_location, file_name)
            # Kiểm tra nếu đường dẫn là một file
            if os.path.isfile(file_path):
                # Lấy thời gian chỉnh sửa gần nhất của file
                file_time = datetime.datetime.fromtimestamp(
                    os.path.getmtime(file_path))

                # Kiểm tra file có thuộc ngày hiện tại và có thời gian gần nhất
                if file_time.date() == today_date:
                    if latest_time is None or file_time > latest_time:
                        latest_time = file_time
                        latest_file = file_path

        if latest_file:
            print(f"Latest file found for today: {latest_file}")
        else:
            print("No file found for today in the specified location.")

        return latest_file

    @staticmethod
    def get_file_path_info(file_path):
        # Lấy tên file, kích thước và số dòng dữ liệu từ đường dẫn file_path
        # Kiểm tra đường dẫn file_path
        if os.path.isfile(file_path) and file_path.endswith('.csv'):
            file_name = os.path.basename(file_path)  # lấy tên file
            size = os.path.getsize(file_path)  # lấy kích thước file
            with open(file_path, 'r', newline='') as file:
                # Đếm số dòng trong file CSV
                # Trả về số dòng bỏ qua header
                count = sum(1 for _ in csv.reader(file)) - 1
            return file_name, size, count
        else:
            return None
