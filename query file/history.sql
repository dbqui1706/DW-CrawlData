/* 2024-10-27 09:27:49 [307 ms] */ 
DROP DATABASE DBLaptop;
/* 2024-10-27 09:54:47 [314 ms] */ 
CREATE DATABASE STAGING_LAPTOP;
/* 2024-10-27 09:54:52 [285 ms] */ 
USE STAGING_LAPTOP;
/* 2024-10-27 10:01:48 [356 ms] */ 
CREATE TABLE IF NOT EXISTS laptop_daily (
    index_id INTEGER PRIMARY KEY AUTO_INCREMENT COMMENT 'Unique serial number of each record in the data table',
    id_laptop CHAR(50) NOT NULL UNIQUE COMMENT 'Unique identification code of the laptop product, sourced from the seller\'s SKU',
    source_laptop TEXT NOT NULL COMMENT 'URL path to the product detail page on the seller\'s website',
    name TEXT NOT NULL COMMENT 'Full name and model information of the laptop product',
    brand TEXT COMMENT 'Brand or manufacturer of the laptop',
    img_src TEXT COMMENT 'URL path to the main image of the laptop on the seller\'s website',
    latest_price DECIMAL(15, 2) COMMENT 'Most recent price of the laptop in local currency',
    retail_price DECIMAL(15, 2) COMMENT 'Original retail price of the laptop, if applicable',
    sale DECIMAL(5, 2) COMMENT 'Percentage of discount applied on the latest price, if any',
    is_available BOOLEAN COMMENT 'Availability status of the product (1 = available, 0 = not available)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of when the record was initially created in the database',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of the last update made to the record in the database'
);
/* 2024-10-27 10:37:05 [309 ms] */ 
CREATE PROCEDURE create_daily_log_table()
BEGIN
    -- Generate table name in the format 'laptop_daily_log_YYYY_MM_DD'
    SET @log_table_name = CONCAT('laptop_daily_log_', DATE_FORMAT(CURDATE(), '%Y_%m_%d'));
    -- Define the SQL statement to create the log table dynamically
    SET @create_table_sql = CONCAT('
        CREATE TABLE IF NOT EXISTS ', @log_table_name, ' (
            index_id INT PRIMARY KEY AUTO_INCREMENT COMMENT ''Unique ID for each log entry'',
            id_laptop CHAR(50) NOT NULL COMMENT ''Reference to index_id in laptop_daily table'',
            action_type ENUM(''INSERT'', ''UPDATE'', ''DELETE'', ''SELECT'') COMMENT ''Type of action recorded'',
            before_data JSON COMMENT ''Data state before the action, if applicable'',
            after_data JSON COMMENT ''Data state after the action, if applicable'',
            action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT ''Timestamp when the action was recorded''
        )'
    );
    -- Print the SQL statement for debugging
    SELECT @create_table_sql AS create_table_sql;
    -- Execute the SQL statement
    PREPARE stmt FROM @create_table_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;
/* 2024-10-27 10:37:16 [326 ms] */ 
CALL create_daily_log_table ();
/* 2024-10-27 11:00:09 [285 ms] */ 
USE STAGING_LAPTOP;
/* 2024-10-27 11:00:21 [286 ms] */ 
CALL create_daily_log_table();
/* 2024-10-27 11:04:14 [311 ms] */ 
CREATE PROCEDURE IF NOT EXISTS delete_old_log_tables()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE table_name VARCHAR(255);
    DECLARE cur CURSOR FOR 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
          AND table_name LIKE 'laptop_daily_log_%'
          AND STR_TO_DATE(SUBSTRING(table_name, 17, 10), '%Y_%m_%d') < (CURDATE() - INTERVAL 7 DAY);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    -- Mở con trỏ và duyệt qua các bảng cần xóa
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO table_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        -- Xóa từng bảng
        SET @drop_sql = CONCAT('DROP TABLE IF EXISTS ', table_name);
        PREPARE stmt FROM @drop_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END ;
/* 2024-10-27 11:04:35 [284 ms] */ 
CALL delete_old_log_tables();
/* 2024-10-27 11:05:41 [297 ms] */ 
CREATE PROCEDURE IF NOT EXISTS delete_old_log_tables()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE table_name VARCHAR(255);
    DECLARE cur CURSOR FOR 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
          AND table_name LIKE 'laptop_daily_log_%'
          AND STR_TO_DATE(SUBSTRING(table_name, 17, 10), '%Y_%m_%d') < (CURDATE() - INTERVAL 7 DAY);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    -- Mở con trỏ và duyệt qua các bảng cần xóa
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO table_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        -- Xóa từng bảng
        SET @drop_sql = CONCAT('DROP TABLE IF EXISTS ', table_name);
        PREPARE stmt FROM @drop_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END ;
/* 2024-10-27 11:05:49 [281 ms] */ 
CALL delete_old_log_tables();
/* 2024-10-27 12:36:59 [310 ms] */ 
CREATE PROCEDURE create_temp_table()
BEGIN
    -- Tạo bảng tạm
    CREATE TABLE IF NOT EXISTS laptop_temp (
        index_id INT PRIMARY KEY AUTO_INCREMENT,
        id_laptop CHAR(50) NOT NULL UNIQUE,
        source_laptop TEXT NOT NULL,
        name TEXT NOT NULL,
        brand TEXT,
        img_src TEXT,
        latest_price DECIMAL(15, 2),
        retail_price DECIMAL(15, 2),
        sale DECIMAL(5, 2),
        is_available BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
END ;
/* 2024-10-27 12:37:13 [294 ms] */ 
CREATE PROCEDURE load_data_from_csv(IN PATH_CSV VARCHAR(255))
BEGIN
    -- Chèn dữ liệu từ tệp CSV vào bảng tạm
    SET @load_sql = CONCAT('LOAD DATA INFILE ''', PATH_CSV, '''
    INTO TABLE laptop_temp
    FIELDS TERMINATED BY '','' 
    ENCLOSED BY ''"'' 
    LINES TERMINATED BY ''\n'' 
    IGNORE 1 ROWS;');
    PREPARE stmt FROM @load_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;
/* 2024-10-27 12:37:50 [311 ms] */ 
CREATE PROCEDURE insert_data_and_log()
BEGIN
    -- Khai báo biến để lưu trữ thông tin
    DECLARE done INT DEFAULT FALSE;
    DECLARE laptop_id CHAR(50);
    DECLARE laptop_source TEXT;
    DECLARE laptop_name TEXT;
    DECLARE laptop_brand TEXT;
    DECLARE laptop_img_src TEXT;
    DECLARE laptop_latest_price DECIMAL(15, 2);
    DECLARE laptop_retail_price DECIMAL(15, 2);
    DECLARE laptop_sale DECIMAL(5, 2);
    DECLARE laptop_is_available BOOLEAN;
    DECLARE cur CURSOR FOR 
        SELECT id_laptop, source_laptop, name, brand, img_src, latest_price, retail_price, sale, is_available 
        FROM laptop_temp;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO laptop_id, laptop_source, laptop_name, laptop_brand, laptop_img_src, laptop_latest_price, laptop_retail_price, laptop_sale, laptop_is_available;
        IF done THEN
            LEAVE read_loop;
        END IF;
        -- Kiểm tra sự tồn tại của bảng log
        CALL check_log_table_exists(@log_exists);
        -- Chỉ ghi log nếu bảng log tồn tại
        IF @log_exists > 0 THEN
            SET @log_sql = CONCAT('INSERT INTO ', @log_table_name, ' (id_laptop, action_type, before_data, after_data, action_timestamp) VALUES 
            (''', laptop_id, ''', ''INSERT'', NULL, JSON_OBJECT(''source_laptop'', ''', laptop_source, ''', ''name'', ''', laptop_name, ''', ''brand'', ''', laptop_brand, ''', ''img_src'', ''', laptop_img_src, ''', ''latest_price'', ', laptop_latest_price, ', ''retail_price'', ', laptop_retail_price, ', ''sale'', ', laptop_sale, ', ''is_available'', ', laptop_is_available, '), CURRENT_TIMESTAMP);');
            PREPARE stmt FROM @log_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
        -- Chèn vào bảng chính
        INSERT INTO laptop_daily (id_laptop, source_laptop, name, brand, img_src, latest_price, retail_price, sale, is_available) VALUES 
        (laptop_id, laptop_source, laptop_name, laptop_brand, laptop_img_src, laptop_latest_price, laptop_retail_price, laptop_sale, laptop_is_available);
    END LOOP;
    CLOSE cur;
    -- Xóa bảng tạm
    DROP TABLE IF EXISTS laptop_temp;
END ;
/* 2024-10-27 12:37:55 [307 ms] */ 
CREATE PROCEDURE insert_laptop_data_from_file_local(IN PATH_CSV VARCHAR(255))
BEGIN
    -- Gọi thủ tục tạo bảng tạm
    CALL create_temp_table();
    -- Gọi thủ tục tải dữ liệu từ CSV
    CALL load_data_from_csv(PATH_CSV);
    -- Gọi thủ tục chèn dữ liệu và ghi log
    CALL insert_data_and_log();
END ;
/* 2024-10-28 12:56:17 [572 ms] */ 
USE STAGING_LAPTOP;
/* 2024-10-28 12:57:39 [273 ms] */ 
CREATE DATABASE IF NOT EXISTS `CONTROL_LAPTOP`;
/* 2024-10-28 12:57:47 [269 ms] */ 
USE `CONTROL_LAPTOP`;
/* 2024-10-28 13:02:46 [332 ms] */ 
CREATE TABLE config_file (
    index_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '',
    name VARCHAR(255) NOT NULL UNIQUE COMMENT '',
    source VARCHAR(255) NOT NULL UNIQUE COMMENT '',
    source_folder_location VARCHAR(255) NOT NULL UNIQUE COMMENT '',
    dest_table_staging VARCHAR(255) NOT NULL COMMENT '',
    dest_table_dw VARCHAR(255) NOT NULL COMMENT '',
    created_at TIMESTAMP NOT NULL COMMENT '',
    updated_at TIMESTAMP COMMENT ''
);
/* 2024-10-28 13:16:21 [612 ms] */ 
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
