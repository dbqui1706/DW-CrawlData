CREATE DATABASE IF NOT EXISTS `CONTROL_LAPTOP`;

USE `CONTROL_LAPTOP`;

-- CREATE TABLES

-- CREATE TABLE CONTROL_FILE

-- Bảng `config_file` lưu cấu hình cho các tệp nguồn và các thông tin liên quan tới lưu trữ và cập nhật dữ liệu vào bảng tạm và kho dữ liệu.

CREATE TABLE IF NOT EXISTS config_file (
    index_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID duy nhất cho mỗi bản ghi cấu hình tệp',
    name VARCHAR(255) NOT NULL UNIQUE COMMENT 'Tên mô tả của cửa hàng laptop',
    source VARCHAR(255) NOT NULL UNIQUE COMMENT 'URL nguồn hoặc đường dẫn đến trang web của cửa hàng',
    source_folder_location VARCHAR(255) NOT NULL UNIQUE COMMENT 'Đường dẫn thư mục chứa tệp được crawl từ cửa hàng',
    dest_table_staging VARCHAR(255) NOT NULL COMMENT 'Tên bảng sẽ chứa dữ liệu được đổ vào của Database STAGING_LAPTOP',
    dest_table_dw VARCHAR(255) NOT NULL COMMENT 'Tên bảng đích trong kho dữ liệu (Data Warehouse)',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo bản ghi cấu hình',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật gần nhất của bản ghi cấu hình'
);

-- Bảng `log_file` ghi lại trạng thái và các thông tin liên quan đến việc tải lên từng tệp, sử dụng khóa ngoại liên kết với bảng `config_file`.

CREATE TABLE IF NOT EXISTS log_file (
    index_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID duy nhất cho mỗi bản ghi log của tệp',
    config_file_id INT NOT NULL COMMENT 'ID tham chiếu tới tệp cấu hình trong bảng config_file',
    file_name VARCHAR(255) COMMENT 'Tên của tệp được tải lên',
    file_size VARCHAR(255) COMMENT 'Kích thước của tệp tải lên (dạng chuỗi để dễ đọc, ví dụ: "10 MB")',
    status ENUM(
        'PENDING',
        'COMPLETED',
        'IN_PROGRESS',
        'FAILED'
    ) NOT NULL COMMENT 'Trạng thái của quá trình xử lý dữ liệu từ file vào DB: PENDING (chờ), COMPLETED (hoàn tất), IN_PROGRESS (đang tiến hành), FAILED (thất bại)',
    count BIGINT COMMENT 'Tổng số dòng dữ liệu đang có trong file',
    row_inserted BIGINT COMMENT 'Số dòng dữ liệu insert hoàn tất vào DB',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo bản ghi log',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật gần nhất của bản ghi log',
    FOREIGN KEY (config_file_id) REFERENCES config_file (index_id) ON DELETE CASCADE
);

-- INSERT dữ liệu vào bảng config_file

INSERT INTO
    config_file (
        name,
        source,
        source_folder_location,
        dest_table_staging,
        dest_table_dw
    )
VALUES (
        'Phong vu',
        'https://phongvu.vn/sitemap.xml',
        'D:\\University\\2024-2025\\kh1_4th\\data warehouse\\data\\phongvu',
        'laptop_daily',
        'laptop_dw'
    )
    -- INSERT dữ liệu vào bảng file_log
INSERT INTO
    log_file (config_file_id, status)
VALUES (1, 'PENDING')