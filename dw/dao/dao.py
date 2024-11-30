from abc import ABC
from database.connection import DBConnection


class DaoConfiguration:
    def __init__(self, db: DBConnection):
        self.connection = db.get_connection()

    def close(self):
        self.db.close()

    def get_configurations(self):
        # Lấy ra các config
        cursor = self.connection.cursor(dictionary=True)

        # Gọi stored procedure
        try:
            procedure_name = "control.get_configurations"
            cursor.callproc(procedure_name)

            # Lấy kết quả từ stored procedure
            configurations = []
            for result in cursor.stored_results():
                # Thêm tất cả các dòng kết quả vào danh sách
                configurations.extend(result.fetchall())

            return configurations
        except Exception as e:
            print(f"Error in get_configurations(): {e}")
            return None
        finally:
            cursor.close()

    def load_data_from_file_to_table(self, config):
        # Load data từ file csv vào bảng tb_temp_staging của DB staging

        # 10.1 Lấy ra đường dẫn chứa file csv.
        source_folder_location = config['source_folder_location'].replace(
            '\\', '\\\\')
        file_name = config['file_name'].replace(
            '\\', '\\\\')
        source = source_folder_location + "\\\\" + file_name

        # Dùng autocommit để thực hiện các lệnh cập nhật đồng thời
        query = f"""
            LOAD DATA LOCAL INFILE '{source}'
            INTO TABLE staging.tb_temp_staging
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\r\n'
            IGNORE 1 ROWS;
        """
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            self.connection.commit()
            print(f"Data loaded successfully from {source}")
            return True
        except Exception as e:
            print(f"Error in load_data_from_file_to_table(): {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def truncate_tb_tmp_staging(self):
        # TODO: truncate dữ liệu trong bảng staging

        query = f"TRUNCATE TABLE staging.tb_temp_staging;"
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            self.connection.commit()
            print("Table staging.tb_temp_staging truncated successfully")
        except Exception as e:
            print(f"Error in function truncate_tb_tmp_staging(): {e}")
        finally:
            cursor.close()

    def truncate_tb_staging(self, config):
        cursor = self.connection.cursor(dictionary=True)

        query = f"TRUNCATE TABLE {config['dest_tb_staging']};"

        try:
            cursor.execute(query)
            self.connection.commit()
            print("Table staging.tb_staging truncated successfully")
        except Exception as e:
            print(f"Error in truncate_tb_staging(): {e}")
        finally:
            cursor.close()

    def transform_data_to_staging(self, config):

        cursor = self.connection.cursor(dictionary=True)
        try:
            procedure_name = "staging.insert_data_staging_from_temp"
            cursor.callproc(procedure_name, (config['dest_tb_staging'],))

            # Commit thay đổi (nếu cần)
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error in transform_data_to_staging(): {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def update_status_code(self, status, id):
        # Thực hiện update status code cho table contro.tb_log
        cursor = self.connection.cursor(dictionary=True)

        try:
            procedure_name = "control.update_status_code"
            cursor.callproc(procedure_name, (id, status))
            # commit thay đổi
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Error in update_status_code(): {e}")
        finally:
            cursor.close()

    def get_status_code(self, config):
        # Thực hiện lấy status code từ table contro.tb_log
        cursor = self.connection.cursor(dictionary=True)
        query = """
            SELECT status_code
            FROM control.tb_log
            WHERE id = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            cursor.execute(query, (config['id'],))
            result = cursor.fetchone()

            return result['status_code'] if result else None
        except Exception as e:
            print(f"Error in get_status_code(): {e}")
            return None
        finally:
            cursor.close()

    def transform_data_to_dw(self, config):
        cursor = self.connection.cursor(dictionary=True)
        # Gọi stored procedure
        try:
            procedure_name = "dw.update_or_insert_dim_product"
            cursor.callproc(procedure_name, (config['dest_tb_staging'],))
            # Commit thay đổi
            self.connection.commit()

            return True
        except Exception as e:
            print(f"Error in transform_data_to_dw(): {e}")
            self.connection.rollback()

            return False
        finally:
            cursor.close()

    def load_data_to_fact(self, config):
        # TODO: Cập nhật dữ liệu vào fact product từ DB dw

        # Gọi stored procedure
        try:
            procedure_name = "dw.load_fact_product"
            cursor = self.connection.cursor(dictionary=True)
            cursor.callproc(procedure_name, (config['dest_tb_staging'],))
            # Commit thay đổi
            self.connection.commit()

            return True
        except Exception as e:
            print(f"Error in load_data_to_fact(): {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def get_configuration_by_id(self, config_id):
        cursor = self.connection.cursor(dictionary=True)

        query = f"""
            SELECT l.id, l.id_config, c.store, l.status_code, l.created_at, c.source_folder_location, 
            c.dest_tb_dw, c.dest_tb_staging, l.file_name
            FROM control.tb_config_file c 
            JOIN control.tb_log l on c.id = l.id_config
            WHERE l.id = %s;
            ORDER BY l.id DESC
            LIMIT 2;
        """
        try:
            cursor.execute(query, (config_id,))
            result = cursor.fetchone()
            return result if result else None
        except Exception as e:
            print(f"Error in get_configuration_by_id(): {e}")
            return None
        finally:
            cursor.close()
