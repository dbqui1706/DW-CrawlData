CREATE DATABASE IF NOT EXISTS control;

-- CONTROLLER DATABASE
-- Tạo bảng config file
-- DROP TABLE IF EXISTS control.tb_config_file;
CREATE TABLE IF NOT EXISTS control.tb_config_file(
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID duy nhất cho mỗi bản ghi cấu hình tệp',
    store CHAR(20) NOT NULL UNIQUE COMMENT 'Tên mô tả của cửa hàng laptop',
    source TEXT NOT NULL COMMENT 'URL nguồn hoặc đường dẫn đến trang web của cửa hàng',
    source_folder_location VARCHAR(255) NOT NULL UNIQUE COMMENT 'Đường dẫn thư mục chứa tệp được crawl từ cửa hàng',
    dest_tb_staging VARCHAR(255) NOT NULL COMMENT 'Tên bảng sẽ chứa dữ liệu được đổ vào của Database STAGING_LAPTOP',
    dest_tb_dw VARCHAR(255) NOT NULL COMMENT 'Tên bảng đích trong Data Warehouse',		
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo bản ghi cấu hình',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật gần nhất của bản ghi cấu hình'
);

-- INSERT dữ liệu vào config file
-- INSERT INTO control.tb_config_file (store, source, source_folder_location, dest_tb_staging, dest_tb_dw) VALUES 
-- ('PV', 'https://phongvu.vn/sitemap_collection_products_4-laptop.xml', 'D:\\dw\\data\\pv', 'staging.tb_staging', 'dw.dim_product'),
-- ('FPT', 'https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml', 'D:\\dw\\data\\fpt', 'staging.tb_staging', 'dw.dim_product');
INSERT INTO
    control.tb_config_file (store, source, source_folder_location, dest_tb_staging, dest_tb_dw)
select 
        'PV', 'https://phongvu.vn/sitemap_collection_products_4-laptop.xml', 'D:\\dw\\data\\pv', 'staging.tb_staging_pv', 'dw.dim_product'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1
    FROM control.tb_config_file
    WHERE store = 'PV'
);    

INSERT INTO
    control.tb_config_file (store, source, source_folder_location, dest_tb_staging, dest_tb_dw)
select 
        'FPT', 'https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml', 'D:\\dw\\data\\fpt', 'staging.tb_staging_fpt', 'dw.dim_product'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1
    FROM control.tb_config_file
    WHERE store = 'FPT'
);    

-- DROP TABLE IF EXISTS control.tb_log;
CREATE TABLE IF NOT EXISTS control.tb_log(
	id int primary key auto_increment,
	id_config int not null,
    file_name varchar(200) NOT NULL default 'STORE_YYYY-MM-DD',
	status_code char(100) not null default 'ER',
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_config) REFERENCES control.tb_config_file(id)
);

-- Load data từ file csv vào bảng tạm
SET GLOBAL local_infile = 1;

-- PROCEDURES
-- 1. LẤY RA CÁC CONFIG 
DROP PROCEDURE IF EXISTS control.get_configurations;
DELIMITER $$
CREATE PROCEDURE control.get_configurations()
BEGIN
	SELECT l.id, l.id_config, c.source,c.store, l.status_code, l.created_at, c.source_folder_location, 
	c.dest_tb_dw, c.dest_tb_staging, l.file_name
	FROM control.tb_config_file c 
	JOIN control.tb_log l on c.id = l.id_config
	-- where DATE(l.created_at) = date(now());
    ORDER BY l.id DESC
    LIMIT 2;
END $$

DELIMITER ;

-- call control.get_configurations();

-- 2. Tạo một stored procedure tự động chèn một bản ghi vào bảng
-- control.tb_log cho mỗi dòng dữ liệu trong bảng control.tb_config_file
DROP PROCEDURE IF EXISTS control.insert_log_from_config;
DELIMITER $$

CREATE PROCEDURE control.insert_log_from_config()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE config_id INT;
    DECLARE config_store VARCHAR(100);
    DECLARE file_name_template VARCHAR(200);
    DECLARE cur CURSOR FOR
        SELECT id, store FROM control.tb_config_file;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO config_id, config_store;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Kiểm tra nếu đã tồn tại bản ghi với id_config và ngày hiện tại
        IF NOT EXISTS (
            SELECT 1 
            FROM control.tb_log 
            WHERE id_config = config_id 
              AND DATE(created_at) = DATE(NOW())
        ) THEN
            -- Tạo file_name theo định dạng STORE_YYYY-MM-DD
            SET file_name_template = CONCAT(config_store, '_', DATE_FORMAT(NOW(), '%Y-%m-%d'), '.csv');

            -- Insert vào tb_log
            INSERT INTO control.tb_log (id_config, file_name)
            VALUES (config_id, file_name_template);
        END IF;
    END LOOP;
    CLOSE cur;
END $$

DELIMITER ;

-- DROP PROCEDURE control.insert_log_from_config;
-- CALL control.insert_log_from_config();

-- 3. Tạo procedure update status code cho bảng control.tb_log
DROP PROCEDURE IF EXISTS control.update_status_code;
DELIMITER $$
CREATE PROCEDURE control.update_status_code(
	IN id_log int,
	IN in_status_code varchar(100)
)
BEGIN 
	UPDATE control.tb_log
    SET status_code = in_status_code
    WHERE id = id_log;
END $$
-- select * from control.tb_log;
-- CALL control.update_status_code(3, 'ER');
-- CALL control.update_status_code(4, 'ER');
