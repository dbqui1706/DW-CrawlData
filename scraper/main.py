from crawler import Crawler
from store.phongvu import PhongVuScraper
from store.fpt import FPTScraper
from writer.csv_writer import CSVWriter
import datetime
import argparse
import os
from dw.database.connection import DBConnection
from dw.dao.dao import DaoConfiguration


def main():
    # 1. Kết nối tới DB
    connection = DBConnection()
    api = DaoConfiguration(connection)

    try:
        # 2. Kiểm tra xem có record cho ngày hôm nay chưa
        configs = api.get_configurations()
        if len(configs) == 0:
            # 2.1 Chưa: Insert record
            api.insert_log_from_config()
            # 2.2 get configurations
            configs = api.get_configurations()

        # 3. Lặp qua các configs
        for config in configs:

            # 4. Kiểm tra trạng thái của status tại log_id = ER
            status = api.get_status_code(config)
            if status != 'ER':
                continue

            # 5. Cào dữ liệu
            store = config['store']
            crawler = Crawler(sitemap_url=config['source'])
            urls = crawler.get_urls()
            products = None
            scraper = None
            source_folder = config['source_folder_location']

            if store == 'PV':
                scraper = PhongVuScraper(urls=urls)
                products = scraper.parse()
            elif store == 'FPT':
                scraper = FPTScraper(urls=urls)
                products = scraper.parse()

            # Nếu không có sản phẩm, tiếp tục với config tiếp theo
            if len(products) == 0:
                continue

            # # 6. Kiểm tra số lượng sản phẩm, nếu ít hơn 100 thì tiếp tục cào thêm
            # if len(products) < 100:
            #     print(f"Số lượng sản phẩm nhỏ hơn 100, tiếp tục cào dữ liệu cho {store}.")
            #
            #     # Cào thêm dữ liệu
            #     while len(products) < 100:
            #         print(f"Hiện tại có {len(products)} sản phẩm. Cào thêm...")
            #
            #         # Tiến hành cào thêm sản phẩm (cào thêm từ trang tiếp theo)
            #         additional_products = scraper.parse()  # Cào thêm sản phẩm
            #         products.extend(additional_products)  # Thêm sản phẩm mới vào danh sách
            #
            #         # Nếu không còn sản phẩm để cào, thoát khỏi vòng lặp
            #         if len(additional_products) == 0:
            #             break
            #
            #         # Giới hạn chỉ lấy 100 sản phẩm
            #         if len(products) > 100:
            #             products = products[:100]  # Cắt số sản phẩm còn lại nếu vượt quá 100

            # api.update_filename_control.tb_log(file_name)

            # 7.1 Update status = EF nếu extract fail
            if len(products) == 0 or products is None:
                api.update_status_code(id=config['id'], status='EF')
                continue

            # 6. Lưu trữ dữ liệu dưới dạng file csv
            current_time = datetime.datetime.now().strftime("%Y-%m-%d")
            file_name = os.path.join(source_folder, f"{store.upper()}_{current_time}.csv")
            writer = CSVWriter()
            os.makedirs(source_folder, exist_ok=True)
            writer.write_to_csv(products, file_name)

            # 7. Update status = LR nếu extract thành công
            api.update_status_code(id=config['id'], status='LR')
            # Đóng scraper
            scraper.close()

    finally:
        # Đảm bảo đóng kết nối
        api.close()


# 8. Tiếp tục config
if __name__ == '__main__':
    main()
#     parser = argparse.ArgumentParser(description='Crawler data for Laptop')
#     parser.add_argument('--store', type=str,
#                         help='Cửa hàng mà bạn muốn craw data', required=True)
#
#     args = parser.parse_args()
#     store = args.store
#     scraper = None
#     sitemap_url = None
#     urls = None
#     if store == 'pv':
#         sitemap_url = "https://phongvu.vn/sitemap_collection_products_4-laptop.xml"
#         sitemap_crawler = Crawler(sitemap_url=sitemap_url)
#         urls = sitemap_crawler.get_urls()
#
#         scraper = PhongVuScraper(urls=urls)
#         products = scraper.parse()
#     elif store == 'fpt':
#         sitemap_url = "https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml"
#         sitemap_crawler = Crawler(sitemap_url=sitemap_url)
#         urls = sitemap_crawler.urls
#
#         scraper = FPTScraper(urls=urls)
#         products = scraper.parse()
#
#         # lấy những url lỗi
#         products_error = []
#         if len(scraper.URL_SERE) > 0:
#             print(f"===> Parsing url ERROR {len(scraper.URL_SERE)}")
#             for url in scraper.URL_SERE:
#                # Remove url error from products
#                # 1. Lấy product có url lỗi trong products
#                 product = next(
#                     (p for p in products if p['source'] == url), None)
#                 if product is not None:
#                     # 2. Xóa product có url lỗi ra khỏi products
#                     products.remove(product)
#                     print(f"Removed product from products: {product['name']}")
#
#                 # Lưu lại url lỗi để sửa sau
#                 products_error.append(scraper.get_product_info(url))
#
#         products.extend(products_error)
#
#     writer = CSVWriter()
#     # write file name contain
#     data_dir = None
#     if store == 'fpt':
#         data_dir = "D:\\dw\\data\\fpt"
#     elif store == 'pv':
#         data_dir = "D:\\dw\\data\\pv"
#
#     current_time = datetime.datetime.now().strftime("%Y-%m-%d")
#     filename = os.path.join(data_dir, f"{store.upper()}_{current_time}.csv")
#
#     # Kiểm tra thư mục tồn tại, nếu không thì tạo mới
#     os.makedirs(data_dir, exist_ok=True)
#
#     writer.write_to_csv(products, filename)
#     scraper.close()
