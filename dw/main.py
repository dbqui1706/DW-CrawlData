from database.connection import DBConnection
from dw.dao.dao import DaoConfiguration
import os


def main():
    # 3. Tạo connection đến database
    db = DBConnection()
    api = DaoConfiguration(db)

    # 4. Lấy ra các config trong ngày có status "ER"
    configs = api.get_configurations()

    # 5. Duyệt for lấy từng config
    for config in configs:
        print(f"Extracting configuration of store {config['store']}")
        # 6. Kiểm tra status của config
        if config['status_code'] != 'ER':
            continue

        # 7. Update status code = PR trong DB Control
        api.update_status_code(id=config['id'], status='PR')

        # 8. Truncate bảng staging.tb_temp_staging
        api.truncate_tb_tmp_staging()
        api.truncate_tb_staging()

        # 9. Load dữ liệu từ file CSV được lấy từ config vào bảng staging.tb_temp_staging
        api.load_data_from_file_to_table(config)

        # 10. Chuyển đổi dữ liệu từ bảng staging.tb_temp_staging sang bảng staging.tb_staging
        affected_rows = api.transform_data_to_staging(config)
        print(f"Load data from Store: {config['store']} successfully!")

        # 11. Kiểm tra status code có bằng T1 ?
        if api.get_status_code(config) != 'T1':
            continue

        # 12. Update status code = PR trong DB Control
        api.update_status_code(id=config['id'], status='PR')
        print(f'====> START TRANSFORM STAGING TO DW')

        # 13. Tranform data from staging.tb_staging to dw.dim_product
        if api.transform_data_to_dw(config) == "EF":
            continue
        print(f'Transforming data from staging to dw.dim_product successfully!')
        # 14. Load data to fact
        if api.load_data_to_fact(config) == "EF":
            continue
        print(f'Load to fact successfully!')

        print('\n================================\n')

    print("Successfully!")


if __name__ == '__main__':
    main()
