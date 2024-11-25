class Config:
    def __init__(self, id_config=None, store=None, source=None, source_folder_location=None,
                 dest_tb_staging=None, dest_tb_dw=None, id=None, file_name=None,
                 status_code=None, created_at=None):
        """
        Khởi tạo đối tượng Config với các thuộc tính liên quan.

        Parameters:
        - id_config: ID cấu hình từ bảng config_file.
        - store: Tên cửa hàng.
        - source: URL nguồn.
        - source_folder_location: Đường dẫn tới thư mục lưu trữ dữ liệu.
        - dest_tb_staging: Bảng staging lưu trữ dữ liệu.
        - dest_tb_dw: Bảng đích trong data warehouse.
        - id: ID bản ghi log (nếu có).
        - file_name: Tên file log.
        - status_code: Trạng thái của log.
        - created_at: Thời gian tạo bản ghi.
        """
        self.id_config = id_config
        self.store = store
        self.source = source
        self.source_folder_location = source_folder_location
        self.dest_tb_staging = dest_tb_staging
        self.dest_tb_dw = dest_tb_dw
        self.id = id
        self.file_name = file_name
        self.status_code = status_code
        self.created_at = created_at

    def from_dict(self, data):
        """Khởi tạo đối tượng từ một dictionary."""
        self.id_config = data.get('id_config')
        self.store = data.get('store')
        self.source = data.get('source')
        self.source_folder_location = data.get('source_folder_location')
        self.dest_tb_staging = data.get('dest_tb_staging')
        self.dest_tb_dw = data.get('dest_tb_dw')
        self.id = data.get('id')
        self.file_name = data.get('file_name')
        self.status_code = data.get('status_code')
        self.created_at = data.get('created_at')
        return self

    def to_dict(self):
        """Trả về một dictionary từ các thuộc tính của đối tượng."""
        return {
            'id_config': self.id_config,
            'store': self.store,
            'source': self.source,
            'source_folder_location': self.source_folder_location,
            'dest_tb_staging': self.dest_tb_staging,
            'dest_tb_dw': self.dest_tb_dw,
            'id': self.id,
            'file_name': self.file_name,
            'status_code': self.status_code,
            'created_at': self.created_at,
        }

    def __str__(self):
        """Hiển thị đối tượng dưới dạng chuỗi."""
        return f"Config(id_config={self.id_config}, store={self.store}, source={self.source}, " \
               f"source_folder_location={self.source_folder_location}, dest_tb_staging={self.dest_tb_staging}, " \
               f"dest_tb_dw={self.dest_tb_dw}, id={self.id}, file_name={self.file_name}, " \
               f"status_code={self.status_code}, created_at={self.created_at})"
