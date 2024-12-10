import argparse
import os
from db_connection import DBConnection
from dao_configuration import DaoConfiguration
from datetime import datetime

def main(args: argparse.Namespace):
    """
    Hàm chính để load dữ liệu từ staging vào bảng aggregate trong data warehouse.
    
    Args:
        args (argparse.Namespace): Tham số truyền vào
            - is_single (int): Chạy nhiều configs hay chỉ 1 config (0 hoặc 1)
            - config_id (int): ID của config cần chạy
    
    Returns:
        None
    """
    # 1. Tạo connection đến database
    db = DBConnection()
    api = DaoConfiguration(db)
    global email
    email = os.getenv("EMAIL")

    if args.is_single == 0:
        # 2. Duyệt qua tất cả các config và thực hiện load_to_aggregate cho từng config
        configs = api.get_configurations()
        for config in configs:
            print(f"Parsing config: {config['id']}")
            load_to_aggregate_process(api, config)
            print("\n****************************************************************")
        print('Dữ liệu đã được load vào DATA WAREHOUSE thành công!')
    else:
        # Khi muốn chạy single configuration
        if args.config_id is None:
            print("Vui lòng nhập ID của config!")
            return
        
        # Lấy ra config muốn chạy
        config = api.get_configuration_by_id(args.config_id)
        # Cập nhật lại status = 'LR' để chạy lại config bị lỗi
        api.update_status_code('LR', config['id'])
        # Tiến hành chạy process load_to_aggregate
        load_to_aggregate_process(api, config)


def aggregate_data(data):
    """
    Hàm thực hiện phép toán aggregate trên dữ liệu.
    Tính tổng doanh thu và tổng số lượng bán của từng sản phẩm.

    Args:
        data (list): Dữ liệu cần tổng hợp từ bảng Staging.

    Returns:
        list: Dữ liệu đã được tổng hợp.
    """
    aggregated_data = []
    
    # Giả sử data là một danh sách các bản ghi, mỗi bản ghi là một dictionary.
    # Tổng hợp dữ liệu theo sản phẩm (ví dụ: tính tổng doanh thu, tổng số lượng bán)
    products = {}
    
    for record in data:
        product_id = record['product_id']
        sales_amount = record['sales_amount']
        quantity_sold = record['quantity_sold']
        
        if product_id not in products:
            products[product_id] = {'total_sales': 0, 'total_quantity': 0}
        
        # Cộng dồn doanh thu và số lượng
        products[product_id]['total_sales'] += sales_amount
        products[product_id]['total_quantity'] += quantity_sold
    
    # Chuyển đổi kết quả thành list của dictionary để insert vào DB
    for product_id, aggregates in products.items():
        aggregated_data.append({
            'product_id': product_id,
            'total_sales': aggregates['total_sales'],
            'total_quantity': aggregates['total_quantity'],
            'aggregated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return aggregated_data


def load_to_aggregate_process(api, config):
    """
    Hàm thực hiện quá trình load dữ liệu từ staging vào bảng aggregate trong Data Warehouse.
    
    Args:
        api (DaoConfiguration): Đối tượng dùng để tương tác với cơ sở dữ liệu
        config (dict): Cấu hình cần chạy
    
    Returns: None
    """
    # 1. Lấy trạng thái config từ cơ sở dữ liệu
    status_code = api.get_status_code(config['id'])
    
    # Kiểm tra xem config có sẵn sàng để load không (status là 'TR' - ready)
    if status_code == 'TR':
        print(f"Config {config['id']} sẵn sàng để load dữ liệu.")

        try:
            # 2. Truy vấn dữ liệu từ bảng staging
            data = api.get_staging_data(config['id'])
            print(f"Lấy được {len(data)} dòng dữ liệu từ staging.")

            # 3. Thực hiện phép toán aggregate trên dữ liệu
            aggregated_data = aggregate_data(data)
            print(f"Thực hiện phép toán aggregate thành công, có {len(aggregated_data)} dòng dữ liệu.")

            # 4. Load dữ liệu đã aggregate vào bảng aggregate trong Data Warehouse
            api.load_to_aggregate_table(aggregated_data, config['id'])
            print(f"Dữ liệu đã được load vào bảng aggregate cho config {config['id']}.")

            # 5. Cập nhật trạng thái thành công trong cơ sở dữ liệu
            api.update_status_code('TS', config['id'])  # 'TS' = Transformed Successfully
        except Exception as e:
            print(f"Lỗi trong quá trình load_to_aggregate: {e}")
            api.update_status_code('EF', config['id'])  # 'EF' = Error
    else:
        print(f"Config {config['id']} không sẵn sàng để load dữ liệu (trạng thái: {status_code}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--is_single", type=int, required=True, help="Chạy một cấu hình riêng biệt (1) hoặc nhiều cấu hình (0)")
    parser.add_argument("--config_id", type=int, help="ID của config cần chạy nếu is_single = 1")
    
    args = parser.parse_args()
    main(args)
