create database staging;
create database control;
create database dw;

-- STAGING DATABASE
-- Tạo table staging;		
-- DROP TABLE staging.tb_staging;
create table if not exists staging.tb_staging(
	id CHAR(50) NOT NULL UNIQUE COMMENT 'Mã nhận dạng duy nhất của sản phẩm laptop',
    store CHAR(20) NOT NULL  default 'UNK' COMMENT 'Tên của của hàng',
    source TEXT  default 'UNK' NOT NULL COMMENT 'Đường dẫn URL tới trang chi tiết sản phẩm trên website của cửa hàng bán',
    name TEXT NOT NULL  default 'UNK' COMMENT 'Tên đầy đủ và thông tin model của sản phẩm Laptop',
    brand VARCHAR(50) NOT NULL default 'UNK' COMMENT 'Thương hiệu hoặc nhà sản xuất máy tính xách tay',
    img_src TEXT NOT NULL COMMENT 'Đường dẫn URL tới hình ảnh chính của laptop trên website của cửa hàng',
    latest_price DECIMAL(15, 2) NOT NULL COMMENT 'Giá hiện tại đang được bán',
    retail_price DECIMAL(15, 2) NOT NULL COMMENT 'Giá gốc của sản phẩm',
    sale DECIMAL(5, 2) NOT NULL COMMENT 'Tỷ lệ chiết khấu áp dụng trên giá mới nhất nếu có',
    is_available BOOLEAN NOT NULL COMMENT 'Tình trạng sẵn có của sản phẩm (1 = có sẵn, 0 = không có sẵn)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Dấu thời gian khi bản ghi ban đầu được tạo trong cơ sở dữ liệu',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Dấu thời gian của lần cập nhật cuối cùng được thực hiện đối với bản ghi trong cơ sở dữ liệu'
);

-- Tạo bảng tạm để load dữ liệu từ file vào
DROP TABLE staging.tb_temp_staging;
create table if not exists staging.tb_temp_staging(
	id TEXT,
    store TEXT ,
    source TEXT  ,
    name TEXT  ,
    brand TEXT  ,
    img_src TEXT  ,
    latest_price TEXT ,
    retail_price TEXT,
    sale TEXT ,
    is_available TEXT
);

-- INSERT dữ liệu từ bảng tạm qua staging
DELIMITER $$

CREATE PROCEDURE staging.insert_data_staging_from_temp()
BEGIN
    INSERT INTO staging.tb_staging (id, store, source, name, brand,
    img_src, latest_price, retail_price, sale, is_available)
    SELECT 
        id, 
        IFNULL(NULLIF(store, ''), 'UNK') AS store, 
        IFNULL(NULLIF(source, ''), 'UNK') AS source, 
        IFNULL(NULLIF(name, ''), 'UNK') AS name, 
        IFNULL(NULLIF(brand, ''), 'UNK') AS brand, 
        IFNULL(NULLIF(img_src, ''), 'UNK') AS img_src, 
        IFNULL(CAST(REPLACE(latest_price, '.', '') AS DECIMAL(15, 2)), 0) AS latest_price, 
        IFNULL(CAST(REPLACE(retail_price, '.', '') AS DECIMAL(15, 2)), 0) AS retail_price, 
        IFNULL(CAST(sale AS DECIMAL(5, 2)), 0)  AS sale, 
        CASE 
            WHEN is_available = 'True' THEN 1
            WHEN is_available = 'False' THEN 0
            ELSE 0 
        END AS is_available
    FROM staging.tb_temp_staging
    WHERE id <> '' 
    ON DUPLICATE KEY UPDATE 
        store = VALUES(store),
        source = VALUES(source),
        name = VALUES(name),
        brand = VALUES(brand),
        img_src = VALUES(img_src),
        latest_price = VALUES(latest_price),
        retail_price = VALUES(retail_price),
        sale = VALUES(sale),
        is_available = VALUES(is_available),
        updated_at = CURRENT_TIMESTAMP;

    -- After INSERT, get the number of affected rows
    SELECT ROW_COUNT() AS affected_rows;
END $$

DELIMITER ;
CALL STAGING.insert_data_staging_from_temp;
-- DROP PROCEDURE STAGING.insert_data_staging_from_temp;




