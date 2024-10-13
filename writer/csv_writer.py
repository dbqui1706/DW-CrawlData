import csv


class CSVWriter:

    def write_to_csv(self, products, filename=None):
        # Bước 1: Tạo tập SET chứa tất cả các keys của sản phẩm

        all_keys = set()
        for product in products:
            all_keys.update(product.keys())

        # Bước 2: Ghi dữ liệu vào file CSV
        print(f"===> Writing to {filename}")
        with open(filename.replace(":", "-"), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_keys)
            writer.writeheader()

            for product in products:
                # Bước 3: Điền giá trị None cho các thuộc tính không có
                row = {key: product.get(key, None) for key in all_keys}
                writer.writerow(row)

        print(f"Finish writing!")
