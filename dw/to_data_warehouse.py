import argparse
from dao.dao import DaoConfiguration
from database.connection import DBConnection
import os
from utils.utils import send_email


def main(args: argparse.Namespace):
    """
    Hàm chính để transform dữ liệu từ staging vào data warehouse
    Args:
        args (argparse.Namespace): Tham số truyền vào
            - is_single (int): Chạy nhiều configs hay chỉ 1 config (0 hoặc 1)
            - config_id (int): ID của config cần chạy
    Returns: None
    """
    # 1. Tạo connection đến database
    db = DBConnection()
    api = DaoConfiguration(db)
    global email
    email = os.getenv("EMAIL")
    if args.is_single == 0:
        # 2. Lấy ra tất cả các configs
        configs = api.get_configurations()
        # 3. Duyệt for lấy từng config
        for config in configs:
            print(f"Parsing config: {config['id']}")
            tranform_process(api, config)
            print("\n****************************************************************")

        print('Transform to DATA WAREHOUSE successfully!')
    else:
        # Khi muốn chạy single configuration
        if args.config_id is None:
            print("Vui lòng nhập ID của config!")
            return

        # Lấy ra config muốn chạy
        config = api.get_configuration_by_id(args.config_id)
        # Cập nhật lại status = 'LR' để chạy lại config bị lỗi
        api.update_status_code('LR', config['id'])
        # Tiến hành chạy process transform từ file vào Staging
        tranform_process(api, config)


def tranform_process(api: DaoConfiguration, config):
    """
    Hàm chuyển đổi dữ liệu từ staging vào data warehouse
    Args:
        api (DaoConfiguration): API để thao tác với database
        config (dict): config cần xử lý
    Returns: None
    """
    # 4. Kiểm tra status của config = TR (transform ready)
    if api.get_status_code(config) != 'TR':
        return
    # 5. Load data từ staging vào data warehouse
    print(
        f"Transforming data from {config['dest_tb_staging']} to {config['dest_tb_dw']}")
    if api.transform_data_to_dw(config) is False:
        print(f"Error in transform_data_to_dw() with config {config['id']}")

        # 6. Cập nhật status code = 'EF' nếu lỗi
        api.update_status_code("EF", config['id'])
        # 7. Gửi email thông báo lỗi
        send_email(email, config['id'], 'DATA WAREHOUSE',
                   f"Error in transform_data_to_dw() with config {config['id']}")
        return

    # 8. Load data vào fact table
    print(f"Loading data to fact table")
    if api.load_data_to_fact(config) is False:
        print(f"Error in load_data_to_fact() with config {config['id']}")

        # 9. Cập nhật status code = 'EF' nếu lỗi
        api.update_status_code("EF", config['id'])
        # 10. Gửi email thông báo lỗi
        send_email(email, config['id'], 'DATA WAREHOUSE',
                   f"Error in load_data_to_fact() with config {config['id']}")
        return

    # 11. Cập nhật status code = 'TS' nếu thành công
    api.update_status_code("TS", config['id'])
    print(f"Transform config {config['id']} successfully")


if __name__ == "__main__":
    paser = argparse.ArgumentParser()
    paser.add_argument("--is_single", type=int, default=0,
                       help="Run multiple configs or only 1 config")
    paser.add_argument("--email", type=str, default=None,
                       help="Email receiver", required=True)
    paser.add_argument("--config_id", type=int, default=None,
                       help="ID of config need to run")

    args = paser.parse_args()
    main(args)
