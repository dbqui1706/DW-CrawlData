-- Tạo cơ sở dữ liệu STAGING
CREATE DATABASE IF NOT EXISTS STAGING_LAPTOP;

-- Chuyển đến cơ sở dữ liệu STAGING
USE STAGING_LAPTOP;

-- Tạo bảng laptop_daily
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

DELIMITER / /

-- Tạo thủ tục create_daily_log_table
CREATE PROCEDURE create_daily_log_table()
BEGIN
    -- Tạo tên bảng log
    SET @log_table_name = CONCAT('laptop_daily_log_', DATE_FORMAT(CURDATE(), '%Y_%m_%d'));
    
    -- Định nghĩa câu lệnh SQL để tạo bảng log động
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

    -- In câu lệnh SQL để kiểm tra
    SELECT @create_table_sql AS create_table_sql;

    -- Thực thi câu lệnh SQL
    PREPARE stmt FROM @create_table_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER;

-- Gọi thủ tục để tạo bảng log hàng ngày
CALL create_daily_log_table ();

DELIMITER / /
-- Tạo thủ tục delete_old_log_tables
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
END //

DELIMITER / /

-- Gọi thủ tục để xóa các bảng log cũ
CALL delete_old_log_tables ();

-- INSERT dữ liệu từ file .csv
-- 1. Khai báo đường dẫn file .csv
SET
    @path_csv = 'D:\University\2024-2025\kh1_4th\data warehouse\data\phongvu\phongvu_2024-10-26_11-55-24.csv'

DELIMITER / /

CREATE PROCEDURE IF NOT EXISTS insert_laptop_data_from_file_local(
    IN PATH_CSV VARCHAR(255),
    IN TABLE_NAME VARCHAR(255)
)
BEGIN
    -- Kiểm tra xem bảng có tồn tại không
    SET @check_table_sql = CONCAT(
        'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ''',
         TABLE_NAME, ''''
    );
    SET @result = 0;
    
    PREPARE check_stmt FROM @check_table_sql;
    EXECUTE check_stmt INTO @result;
    DEALLOCATE PREPARE check_stmt;

    -- Nếu bảng không tồn tại, thông báo lỗi
    IF @result = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Table does not exist';
    ELSE
        -- Chèn dữ liệu vào bảng từ file CSV
        SET @load_data_sql = CONCAT('LOAD DATA LOCAL INFILE ''', PATH_CSV, '''
                                    INTO TABLE ', TABLE_NAME, '
                                    FIELDS TERMINATED BY '','' 
                                    ENCLOSED BY ''"''
                                    LINES TERMINATED BY ''\n''
                                    IGNORE 1 ROWS;');

        -- Thực hiện câu lệnh LOAD DATA
        PREPARE load_stmt FROM @load_data_sql;
        EXECUTE load_stmt;
        DEALLOCATE PREPARE load_stmt;
    END IF;
END //

DELIMITER / /

DELIMITER / /

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
END //

DELIMITER / /

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
END //

DELIMITER / /

CREATE PROCEDURE check_log_table_exists(OUT log_exists INT)
BEGIN
    SET @log_table_name = CONCAT('Log_daily_', DATE_FORMAT(CURDATE(), '%Y_%m_%d'));
    SET @log_exists_sql = CONCAT('SELECT COUNT(*) INTO log_exists FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ''', @log_table_name, ''';');
    PREPARE stmt FROM @log_exists_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER / /

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
END //

DELIMITER / /

CREATE PROCEDURE insert_laptop_data_from_file_local(IN PATH_CSV VARCHAR(255))
BEGIN
    -- Gọi thủ tục tạo bảng tạm
    CALL create_temp_table();

    -- Gọi thủ tục tải dữ liệu từ CSV
    CALL load_data_from_csv(PATH_CSV);

    -- Gọi thủ tục chèn dữ liệu và ghi log
    CALL insert_data_and_log();
END //

DELIMITER;

CREATE TABLE laptop_temp (
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
)

CREATE TABLE IF NOT EXISTS laptop_daily_log (
    index_id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'Unique ID for each log entry',
    id_laptop CHAR(50) NOT NULL COMMENT 'Reference to index_id in laptop_daily table',
    action_type ENUM(
        'INSERT',
        'UPDATE',
        'DELETE',
        'SELECT'
    ) COMMENT 'Type of action recorded',
    before_data JSON COMMENT 'Data state before the action, if applicable',
    after_data JSON COMMENT 'Data state after the action, if applicable',
    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the action was recorded'
)

DELIMITER / /

DROP TRIGGER IF EXISTS after_laptop_daily_insert;

CREATE TRIGGER after_laptop_daily_insert
AFTER INSERT ON laptop_daily_log
FOR EACH ROW
BEGIN
    INSERT INTO laptop_daily_log (id_laptop, action_type, before_data, after_data, action_timestamp)
    VALUES (
        NEW.id_laptop,
        'INSERT',
        JSON_OBJECT(),
        JSON_OBJECT(
            'id_laptop', NEW.id_laptop,
            'source_laptop', NEW.source_laptop,
            'name', NEW.name,
            'brand', NEW.brand,
            'img_src', NEW.img_src,
            'latest_price', NEW.latest_price,
            'retail_price', NEW.retail_price,
            'sale', NEW.sale,
            'is_available', NEW.is_available,
            'created_at', NEW.created_at,
            'updated_at', NEW.updated_at
        ),
        CURRENT_TIMESTAMP
    );
END//

DELIMITER / /

INSERT INTO
    laptop_temp (
        id_laptop,
        source_laptop,
        name,
        brand,
        img_src,
        latest_price,
        retail_price,
        sale,
        is_available
    )
VALUES (
        'LAPTOP12345', -- id_laptop (SKU của sản phẩm)
        'https://example.com/laptop12345', -- source_laptop (URL của sản phẩm)
        'Dell XPS 13', -- name (Tên và model của laptop)
        'Dell', -- brand (Thương hiệu của laptop)
        'https://example.com/images/laptop12345.jpg', -- img_src (URL hình ảnh của sản phẩm)
        1200.00, -- latest_price (Giá mới nhất)
        1500.00, -- retail_price (Giá gốc)
        20.00, -- sale (Phần trăm giảm giá)
        1 -- is_available (Tình trạng còn hàng)
    );

DELIMITER;