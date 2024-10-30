

class Staging:

    def __init__(self, db_connection):
        self.db_connection = db_connection

    def insert_to_tb_laptop_daily(self):

        # 1. connect to CONTROL_LAPTOP

        # 2. Lấy những cửa hàng có trong table config_file
        # 2.1 Lấy trường `source_folder_location` và lấy ra file mới nhất có trong ngày hiện tại
        # 2.1.1 Nếu không có thì tiếp tục store khác
        # 2.1.2 Nếu có thì lấy đường dẫn của file đó và ID của table config_file
        # 2.2 Kiểm tra xem ID của config_file đã được tạo trong table log_file trong ngày hiện tại chưa
        # 2.2.1 Nếu chưa tồn tại thì tạo log và thêm ID vào table log_file và set STATUS == 'PENDING'
        # 2.2.2 Nếu đã tồn tại thì tiếp tục
        # 2.3 tiến hành kiểm tra STATUS
        # 2.3.1 Nếu STATUS == 'PENDING' update STATUS = 'IN_PROGRESS', count = số lượng dòng dữ liệu có trong file_name
        # 2.3.2 Nếu STATUS thuộc 3 trạng thái còn lại thì không cần làm gì quay về bước 2.1
        # try
        # 2.4 Connect đến DB_STAGING
        # 2.5  Tiến hành insert dữ liệu vào TABLE laptop_daily
        # 2.6 Loop từng dòng dữ liệu.
        # 2.6.1 Nếu đã tồn tại trong TABLE laptop_daily và không có gì thay đổi trong các trường dữ liệu -> Tiếp tục
        # 2.6.2 Nếu chưa tồn tại -> Insert data vào table laptop_daily và ghi log `INSERT` vào table `log_laptop_daily của ngày hiện tại`
        # 2.6.3 Nếu đã tồn tại và có trường dữ liệu thay đổi -> Update dữ liệu và ghi log `UPDATE` vào table `log_laptop_daily của ngày hiện tại`
        # catch
        # Nếu có bất cứ lỗi gì trong quá trình thao tác với dữ liệu từ file thì sẽ tiếp tục quay về bước 2.1
        # finally
        # 2.7.1 Nếu STATUS == 'IN_PROGRESS' thì update STATUS = 'COMPLETED' và row_inserted = số lượng dòng đã insert vào table laptop_daily
        # 2.7.2 Nếu STATUS == 'FAILED' thì update STATUS = 'FAILED' và row_inserted = số lượng dòng đã insert vào table laptop_daily
        # 2.7.3 Nếu STATUS == 'COMPLETED' thì update STATUS = 'COMPLETED' và row_inserted = số lượng dòng đã insert vào table laptop_daily
        # 2.7.4 Nếu STATUS == 'FAILED' thì update STATUS = 'FAILED' và row_inserted = số lượng dòng đã insert vào table laptop_daily

        pass
