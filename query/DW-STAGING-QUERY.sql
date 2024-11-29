-- Create a new DATABASE 
CREATE DATABASE STAGING_LAPTOP;
-- USING DATABASE
USE STAGING_LAPTOP;

-- Create tables
-- 1. Create table laptop_daily
CREATE TABLE IF NOT EXISTS laptop_daily(
	id CHAR(50) NOT NULL PRIMARY KEY COMMENT 'Unique identification code of the laptop product, sourced from the seller\'s',
    store VARCHAR(50) NOT NULL COMMENT 'Store name laptop website',
    source_laptop TEXT NOT NULL COMMENT 'URL path to the product detail page on the seller\'s website',
    laptop_name TEXT NOT NULL COMMENT 'Full name and model information of the laptop product',
    brand VARCHAR(50) COMMENT 'Brand or manufacturer of the laptop',
    img_src TEXT COMMENT 'URL path to the main image of the laptop on the seller\'s website',
    latest_price DECIMAL(15, 2) COMMENT 'Most recent price of the laptop in local currency',
    retail_price DECIMAL(15, 2) COMMENT 'Original retail price of the laptop, if applicable',
    sale DECIMAL(5, 2) COMMENT 'Percentage of discount applied on the latest price, if any',
    is_available BOOLEAN COMMENT 'Availability status of the product (1 = available, 0 = not available)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of when the record was initially created in the database',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of the last update made to the record in the database'
);

-- 2. Create table TEMP have datatype = TEXT
CREATE TABLE IF NOT EXISTS laptop_daily_temp (
    id TEXT,
    store TEXT,
    source_laptop TEXT,
    laptop_name TEXT,
    brand TEXT,
    img_src TEXT,
    latest_price TEXT,
    retail_price TEXT,
    sale TEXT,
    is_available TEXT
);



-- 3. Create procedure to Load file csv to STAGING_LAPTOP.laptop_daily_temp
DELIMITER //

CREATE PROCEDURE load_csv_to_laptop_daily_temp(
    IN file_path TEXT
)
BEGIN
    -- Xóa dữ liệu hiện có trong bảng tạm
    TRUNCATE TABLE laptop_daily_temp;

    -- Nạp dữ liệu từ file CSV vào bảng tạm
    SET @load_data_sql = CONCAT("LOAD DATA LOCAL INFILE '", file_path, "'
                                    INTO TABLE laptop_daily_temp
                                    FIELDS TERMINATED BY ',' 
                                    ENCLOSED BY '\"'
                                    LINES TERMINATED BY '\n'
                                    IGNORE 1 ROWS
                                    (id, store, source_laptop, laptop_name, brand, img_src, latest_price, retail_price, sale, is_available);");
    
    -- Thực hiện câu lệnh LOAD DATA
    PREPARE load_stmt FROM @load_data_sql;
    EXECUTE load_stmt;
    DEALLOCATE PREPARE load_stmt;

    -- Thông báo khi hoàn tất
    SELECT 'Dữ liệu đã được nạp thành công vào laptop_daily_temp';
END //

DELIMITER ;

CALL load_csv_to_laptop_daily_temp('D:/University/2024-2025/kh1_4th/data warehouse/data/phongvu/phongvu_2024-10-28_15-42-59.csv');

LOAD DATA LOCAL INFILE 'D:/University/2024-2025/kh1_4th/data warehouse/data/phongvu/phongvu_2024-10-28_15-42-59.csv'
INTO TABLE laptop_daily_temp
FIELDS TERMINATED BY ',' 
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, store, source_laptop, laptop_name, brand, img_src, latest_price, retail_price, sale, is_available);

LOAD DATA LOCAL INFILE 'D:/University/2024-2025/kh1_4th/data warehouse/data/phongvu/phongvu_2024-10-28_15-42-59.csv'
INTO TABLE laptop_daily_temp
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE laptop_daily_temp;

SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';
