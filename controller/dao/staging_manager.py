from datetime import datetime
from log_manager import LogManager
import pandas as pd


class StaingManager(LogManager):
    def __init__(self):
        super(StaingManager, self).__init__()

    def get_all_record_laptop_daily(self):
        # Lấy tất cả các records có trong laptop_daily
        query = """
        SELECT id_laptop, source_laptop, name, brand, img_src, latest_price, retail_price, sale, is_available
        FROM laptop_daily
        """
        return self.db_connection.execute_query(query, function_name='get_all_record_laptop_daily')

    import pandas as pd

    def insert_data_from_file(self, file_path):
        # Đọc dữ liệu từ file CSV
        try:
            data = pd.read_csv(file_path)
        except Exception as e:
            print(f"Error reading the file: {e}")
            return

        # Tạo cursor
        cursor = self.db_connection.cursor()

        # 1. Tải tất cả id_laptop có sẵn vào bộ nhớ
        existing_records = {
            row[0]: row[1:]
            for row in self.get_all_record_laptop_daily_records()
        }

        # 2. Tạo danh sách cho bản ghi chèn mới và bản ghi cần cập nhật
        insert_values = []
        update_queries = []

        for index, row in data.iterrows():
            id_laptop = row['id']

            if id_laptop in existing_records:
                # Tồn tại bản ghi, kiểm tra sự thay đổi
                update_query, params = self.create_update_query(
                    row, existing_records[id_laptop])
                if update_query:
                    # Store both query and params
                    update_queries.append((update_query, params))
            else:
                # Không tồn tại bản ghi, chèn mới
                insert_values.append((
                    id_laptop,
                    row['source_laptop'],
                    row['name'],
                    row['brand'],
                    row['img_src'],
                    row['latest_price'],
                    row['retail_price'],
                    row['sale'],
                    row['is_available']
                ))

        # 3. Thực hiện chèn hàng loạt
        row_inserted = self._bulk_insert(
            cursor, insert_values) if insert_values else 0

        # 4. Thực hiện cập nhật hàng loạt
        row_updated = 0
        for update_query, params in update_queries:
            try:
                cursor.execute(update_query, params)
                row_updated += 1
                print(f"Updated record for laptop ID: {params[-1]}")
            except:
                continue

        # Cam kết các thay đổi
        self.db_connection.commit()
        cursor.close()
        return {
            'inserted': row_inserted,
            'updated': row_updated
        }

    def create_update_query(self, row, existing_data):
        """Kiểm tra sự thay đổi và tạo truy vấn cập nhật nếu cần."""
        needs_update = False
        updated_fields = []

        # Kiểm tra từng trường để xem có thay đổi không
        fields = ['source_laptop', 'name', 'brand', 'img_src',
                  'latest_price', 'retail_price', 'sale', 'is_available']

        for field_index, field in enumerate(fields):
            if row[field] != existing_data[field_index]:
                # Thêm trường vào danh sách cần cập nhật
                updated_fields.append(f"{field} = %s")
                needs_update = True

        if needs_update:
            # Tạo truy vấn cập nhật với các trường đã thay đổi
            update_query = f"UPDATE laptop_daily SET {', '.join(updated_fields)}, updated_at = CURRENT_TIMESTAMP WHERE id_laptop = %s"

            # Tạo tuple các tham số cho truy vấn
            params = tuple(row[field] for field in fields) + \
                (row['id'],)  # Ensure using the correct id

            return update_query, params  # Trả về câu lệnh và tham số

        return None, None  # Ensure consistent return

    def _bulk_insert(self, cursor, insert_values):
        """Thực hiện chèn hàng loạt vào cơ sở dữ liệu."""
        insert_query = """
        INSERT INTO laptop_daily (id_laptop, source_laptop, name, brand, img_src, latest_price, retail_price, sale, is_available)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, insert_values)
        print(f"Inserted {len(insert_values)} new records.")
        return len(insert_values)
