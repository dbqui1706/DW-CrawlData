-- TABLES
-- 1. CREATE TABLE data_dim
CREATE TABLE IF NOT EXISTS dw.dim_date(
	date_sk INT primary KEY,    			-- Khóa chính, định dạng YYYYMMDD
    date DATE NOT NULL,                     -- Ngày
    year INT NOT NULL,                      -- Năm
    quarter INT NOT NULL,                   -- Quý (1, 2, 3, 4)
    month INT NOT NULL,                     -- Tháng (1-12)
    day INT NOT NULL,                       -- Ngày trong tháng (1-31)
    weekday INT NOT NULL,                   -- Ngày trong tuần (0-6, 0 là Chủ nhật)
    is_weekend BOOLEAN NOT NULL,            -- Cờ cho biết ngày có phải cuối tuần không (1 = cuối tuần, 0 = ngày thường)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. Tạo bảng dim product
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_sk INT NOT NULL AUTO_INCREMENT PRIMARY KEY,   -- Khóa chính cho DIM_PRODUCT
    id CHAR(50) NOT NULL,                                 -- Mã sản phẩm (đến từ `staging.tb_staging`)
    store CHAR(20) NOT NULL DEFAULT 'UNK',                -- Cửa hàng
    source TEXT,                            			  -- URL nguồn
    name TEXT NOT NULL,                                   -- Tên sản phẩm
    brand VARCHAR(50) NOT NULL DEFAULT 'UNK',             -- Thương hiệu
    img_src TEXT NOT NULL,                                -- URL hình ảnh sản phẩm
    latest_price DECIMAL(15, 2) NOT NULL,                 -- Giá hiện tại
    retail_price DECIMAL(15, 2) NOT NULL,                 -- Giá gốc
    sale DECIMAL(5, 2) NOT NULL,                          -- Tỷ lệ chiết khấu
    is_available BOOLEAN NOT NULL,                        -- Tình trạng có sẵn
    effective_date DATE NOT NULL,                         -- Ngày bắt đầu hiệu lực của bản ghi
    end_date DATE DEFAULT '9999-12-31',                   -- Ngày kết thúc (mặc định là '9999-12-31' cho bản ghi hiện tại)
    is_expired BOOLEAN NOT NULL DEFAULT TRUE,             -- Cờ đánh dấu bản ghi hiện tại đã hết hạn chưa
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Tạo một bảng fact price change
CREATE TABLE IF NOT EXISTS dw.fact_product (
    pc_sk INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 		-- Khóa chính (fact product)
    product_sk INT NOT NULL, 							-- Khóa ngoại liên kết với `dim_product`
    change_date_sk INT NOT NULL, 						-- Khóa ngoại liên kết với `date_dim`
    store CHAR(50) NOT NULL,              				-- Sự thay đổi Cửa hàng 
    change_source TEXT NOT NULL,                        -- URL nguồn
    change_name TEXT NOT NULL,                          -- Tên sản phẩm
    change_brand VARCHAR(50) NOT NULL,    				-- Thương hiệu
    change_img_src TEXT NOT NULL, 						-- URL ảnh
    change_latest_price DECIMAL(15, 2) NOT NULL, 		-- Sự thay đổi giá mới nhất
    change_retail_price DECIMAL(15, 2) NOT NULL, 		-- Sự thay đổi giá gốc
    change_sale DECIMAL(5, 2) NOT NULL, 				-- Sự thay đổi tỷ lệ chiết khấu
    change_is_available BOOLEAN NOT NULL, 				-- Sản phẩm có sẵn không
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_sk) REFERENCES dw.dim_product(product_sk),
    FOREIGN KEY (change_date_sk) REFERENCES dw.dim_date(date_sk)
);

-- ******************************** PROCEDURES ********************************
-- Tạo procedure cho 1 lần chạy duy nhất và không sử dụng lại
-- để tạo dữ liệu cho bảng dw.date_dim
-- Dữ liệu thời gian từ 2024-01-01 đến 2025-12-31
DELIMITER $$

CREATE PROCEDURE dw.create_data_for_date_dim()
BEGIN
	-- Tạo View cho dãy ngày từ 2024-01-01 đến 2025-12-31
	CREATE VIEW dw.date_range_view AS
	WITH RECURSIVE date_range AS (
		SELECT DATE('2024-01-01') AS date
		UNION ALL
		SELECT DATE_ADD(date, INTERVAL 1 DAY)
		FROM date_range
		WHERE date < DATE('2025-12-31')
	)
	SELECT 
		YEAR(date) * 10000 + MONTH(date) * 100 + DAY(date) AS date_key,
		date,
		YEAR(date) AS year,
		QUARTER(date) AS quarter,
		MONTH(date) AS month,
		DAY(date) AS day,
		DAYOFWEEK(date) - 1 AS weekday,  -- Chủ nhật = 0, Thứ bảy = 6
		CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend -- Chủ nhật hoặc Thứ bảy
	FROM date_range;

	-- Chèn dữ liệu từ View vào bảng dw.date_dim
	INSERT INTO dw.dim_date (date_sk, date, year, quarter, month, day, weekday, is_weekend)
	SELECT date_key, date, year, quarter, month, day, weekday, is_weekend
	FROM dw.date_range_view;
	
    # Xóa view dw.date_range_view
    DROP VIEW IF EXISTS dw.date_range_view;
END $$
DELIMITER ;
-- Gọi procedure để thực thi
CALL dw.create_data_for_date_dim;
select * from dw.dim_date;

-- 1. Tạo procedure cho quá trình insert data từ table 
-- staging.tb_staging sang dw.dim_product nếu là dữ liệu mới
DELIMITER $$
CREATE PROCEDURE dw.insert_new_data_into_dim_product()
BEGIN
	-- Chèn hoặc cập nhật dữ liệu vào bảng dim_product
    INSERT INTO dw.dim_product (
        id, store, source, name, brand, img_src, latest_price, retail_price, sale, 
        is_available, effective_date, end_date, is_expired
    )
    SELECT id, store, source, name, brand, img_src, latest_price, retail_price,
        sale, is_available,
        CURRENT_DATE, 	-- effective_date là ngày hiện tại
        '9999-12-31', 	-- end_date mặc định
        FALSE 			-- is_expired mặc định là FALSE
    FROM staging.tb_staging AS stg -- Gọi đến tb_staging từ DB staging
    WHERE NOT EXISTS (
		SELECT 1
		FROM dw.dim_product p
		WHERE p.id = id
	); 
END $$

DELIMITER ;
-- Gọi và thực thi procedure dw.insert_new_data_into_dim_product
CALL dw.insert_new_data_into_dim_product;

-- 1. Tạo procedure cho quá trình insert data từ table 
-- staging.tb_staging sang dw.dim_product nếu ID đã tồn tại
-- nhưng những trường dữ liệu khác thay đổi như source, name, brand
-- img_src, latest_price, retail_price, sale, is_available
-- tiến hành insert và cập nhật ID đã tồn tại với trường end_date = ngày được thêm 
-- và update is_expired = True
DELIMITER $$

CREATE PROCEDURE dw.update_or_insert_dim_product()
BEGIN
    -- Bước 1: Cập nhật bản ghi cũ nếu ID đã tồn tại và các trường khác thay đổi
    UPDATE dw.dim_product AS dim
    JOIN staging.tb_staging AS stg
    ON dim.id = stg.id
    SET 
        dim.end_date = CURRENT_DATE,      -- Đặt ngày kết thúc cho bản ghi cũ
        dim.is_expired = TRUE,           -- Đánh dấu bản ghi cũ là đã hết hạn
        dim.updated_at = CURRENT_TIMESTAMP
    WHERE dim.is_expired = FALSE         -- Chỉ cập nhật bản ghi hiện tại
    AND (
        dim.source <> stg.source OR
        dim.name <> stg.name OR
        dim.brand <> stg.brand OR
        dim.img_src <> stg.img_src OR
        dim.latest_price <> stg.latest_price OR
        dim.retail_price <> stg.retail_price OR
        dim.sale <> stg.sale OR
        dim.is_available <> stg.is_available
    );

   -- Bước 2: Chèn bản ghi mới từ staging.tb_staging vào dim_product
    INSERT INTO dw.dim_product (
        id, store, source, name, brand, img_src, latest_price, retail_price, sale, 
        is_available, effective_date, end_date, is_expired
    )
    SELECT 
        stg.id, stg.store, stg.source, stg.name, stg.brand, stg.img_src, 
        stg.latest_price, stg.retail_price, stg.sale, stg.is_available, 
        CURRENT_DATE,                -- effective_date là ngày hiện tại
        '9999-12-31',                -- end_date mặc định
        FALSE                        -- is_expired mặc định là FALSE
    FROM staging.tb_staging AS stg
    LEFT JOIN dw.dim_product AS dim
    ON stg.id = dim.id AND dim.is_expired = FALSE
    WHERE dim.product_sk IS NULL;    -- Chỉ thêm bản ghi mới nếu chưa tồn tại
END$$
DELIMITER ;
-- Thực thi procedure để kiểm tra xem hoạt động đúng chưa
Call dw.update_or_insert_dim_product;


-- 2.Kiểm tra sự thay đổi của giá (latest_price, retail_price) 
-- và tỷ lệ chiết khấu (sale) và các trường khác như store 
-- source, name, brand img_src, is_available từ dữ liệu trong dim_product
-- Nếu phát hiện thay đổi, ghi lại sự thay đổi này trong bảng dw.fact_product.
DELIMITER $$

CREATE PROCEDURE dw.load_fact_product()
BEGIN
    -- Chèn dữ liệu vào fact_product khi có thay đổi
    INSERT INTO dw.fact_product (
        product_sk, change_date_sk, store, change_source, change_name, 
        change_brand, change_img_src, change_latest_price, change_retail_price, 
        change_sale, change_is_available
    )
    SELECT 
        dim.product_sk, 
        dd.date_sk, -- Tìm ngày thay đổi từ `dw.date_dim`
        stg.store, stg.source, stg.name, stg.brand, stg.img_src, 
        stg.latest_price, stg.retail_price, stg.sale, stg.is_available
    FROM staging.tb_staging AS stg
    JOIN dw.dim_product AS dim
        ON stg.id = dim.id
    JOIN dw.dim_date AS dd
        ON dd.date = CURRENT_DATE -- Mapping ngày hiện tại từ `date_dim`
    WHERE 
        dim.is_expired = TRUE -- Chỉ lấy bản ghi mới nhất từ `dim_product`
        AND (
            dim.source <> stg.source OR
            dim.name <> stg.name OR
            dim.brand <> stg.brand OR
            dim.img_src <> stg.img_src OR
            dim.latest_price <> stg.latest_price OR
            dim.retail_price <> stg.retail_price OR
            dim.sale <> stg.sale OR
            dim.is_available <> stg.is_available
        );
END$$
DELIMITER ;
CALL dw.load_fact_product;

SELECT store ,count(*) as TOTAL 
FROM dw.dim_product
group by store;

SELECT *
from dw.fact_product;

select *
from dw.dim_product
where id = 'sku.241004352';

