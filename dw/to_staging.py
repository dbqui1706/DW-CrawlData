from database.connection import DBConnection
from dao.dao import DaoConfiguration
import os
import argparse
from utils.utils import *


def main(args):
    """
    Hàm chính để chạy tiền xử lý dữ liệu từ file csv vào database staging
    Args:
        args (argparse.Namespace): Tham số truyền vào
            - is_single (int): Chạy nhiều configs hay chỉ 1 config (0 hoặc 1)
            - email (str): Email của người dùng sẽ nhận được thông báo khi có lỗi
            - config_id (int): ID của config cần chạy
    Returns: None
    """

    # 1. Tạo connection đến database
    db = DBConnection()
    api = DaoConfiguration(db)
    global email
    email = args.email
    if args.is_single == 0:
        # 2. Duyệt for lấy từng config
        configs = api.get_configurations()
        for index, config in enumerate(configs):
            print(f"Parsing config: {index}")
            preprocess_config(api, config)

            print("\n****************************************************************")
        print('Transform to STAGING successfully!')
    else:
        # Khi muốn chạy single configuration
        if args.config_id is None:  # Đã sửa lỗi
            print("Vui lòng nhập ID của config!")
            return

        # Lấy ra config muốn chạy
        config = api.get_configuration_by_id(args.config_id)
        # Cập nhật lại status = 'LR' để chạy lại config bị lỗi
        api.update_status_code('LR', config['id'])
        # Tiến hành chạy process transform từ file vào Staging
        preprocess_config(api, config)


def preprocess_config(api: DaoConfiguration, config):
    """
    Hàm chuyển đổi dữ liệu từ file csv vào database staging
    Args:
        api (DaoConfiguration): API để thao tác với database
        config (dict): config cần xử lý

    Returns: None
    """

    # 3. Kiểm tra status của config
    if api.get_status_code(config) != 'LR':
        return
    # 4. Truncate table staging, table temp
    api.truncate_tb_tmp_staging()
    print(f"Truncate table `{config['dest_tb_staging']}`")
    api.truncate_tb_staging(config)

    # 5. Load dữ liệu từ file csv vào table temp
    print(
        f"Loading file {config['file_name']} ===> {config['dest_tb_staging']}")
    if api.load_data_from_file_to_table(config) is False:  # Khi thất bại
        # 6. cập nhập status = LF
        print(
            f"[Failed]: Load {config['file_name']} ===> {config['dest_tb_staging']}")
        api.update_status_code("LF", config['id'])

        # Gửi email thông báo lỗi
        send_email(email, config['id'], 'STAGING',
                   "Load dữ liệu từ file csv vào table temp",)
        return

    # 7. Load dữ từ temp vào staging
    print(
        f"Loading data from temp table ===> {config['dest_tb_staging']}")
    if api.transform_data_to_staging(config):
        # 8. cập nhập status = TR
        api.update_status_code("TR", config['id'])
        return
    # 9. cập nhập status = LF nếu thất bại
    api.update_status_code("LF", config['id'])
    # Gửi email thông báo lỗi
    send_email(email, config['id'], 'STAGING',
               "Load dữ từ temp vào staging",)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Load data from file to database STAGING')
    parser.add_argument('--is_single', type=int, default=False,
                        help='Chạy nhiều configs hay chỉ 1 config (0 hoặc 1)', required=True)
    parser.add_argument('--email', type=str, default=None, help='Email của người dùng sẽ nhận được thông báo khi có lỗi',
                        required=True)
    parser.add_argument('--config_id', type=int, default=None,
                        help='ID của config cần chạy', required=False)

    args = parser.parse_args()
    main(args)
