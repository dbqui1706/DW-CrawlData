CREATE TABLE IF NOT EXISTS dw.load_to_aggregate (
    aggregate_sk INT PRIMARY KEY AUTO_INCREMENT,  -- Khóa chính tự động tăng
    config_id INT,                                -- ID của cấu hình
    product_id INT,                               -- ID sản phẩm (khóa phụ liên kết với bảng fact_product)
    pc_sk INT,                                    -- Khóa phụ liên kết với bảng product_category
    total_sales DECIMAL(10, 2),                   -- Tổng doanh thu
    total_quantity INT,                          -- Tổng số lượng
    aggregated_date DATE,                         -- Ngày tính toán
    status_code VARCHAR(20),                      -- Mã trạng thái
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Thời gian tạo bản ghi
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Thời gian cập nhật
    is_active BOOLEAN DEFAULT TRUE,               -- Cờ trạng thái hoạt động
    total_discount DECIMAL(10, 2),                -- Tổng giảm giá
    total_tax DECIMAL(10, 2),                     -- Tổng thuế
    total_profit DECIMAL(10, 2),                  -- Tổng lợi nhuận
    FOREIGN KEY (product_id) REFERENCES dw.fact_product(product_id),  -- Khóa phụ liên kết với fact_product
    FOREIGN KEY (pc_sk) REFERENCES dw.product_category(pc_sk)        -- Khóa phụ liên kết với product_category
);
