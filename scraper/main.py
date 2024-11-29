from crawler import Crawler
from store.phongvu import PhongVuScraper
from store.fpt import FPTScraper
from writer.csv_writer import CSVWriter
import datetime
import argparse
import os
# from dw.database.connection import DBConnection
# from dw.dao.dao import DaoConfiguration


# def main():
#     # 1. Kết nối tới DB
#     connection = DBConnection()
#     api = DaoConfiguration(connection)
#     # 2. Kiểm tra xem có 2 record cho ngày hôm nay chưa
#     configs = api.get_configurations()
#     if len(configs) == 0:
#         # 2.1 Chưa: Insert record
#         # 2.2 get get_configurations
#         pass

#     # 3. Lặp qua các configs
#     for config in configs:
#         # 4. Kiểm tra trạng thái của status tại log_id = ER
#         status = api.get_status_code(config)
#         if status != 'ER':
#             continue
#         # 5. Cào dữ liệu
#         store = config['store']
#         crawler = Crawler(sitemap_url=config['source'])
#         urls = crawler.get_urls()
#         products = None
#         scraper = None
#         file_name = None
#         source_folder = config['source_folder_location']
#         if store == 'PV':
#             scraper = PhongVuScraper(urls=urls)
#             products = scraper.parse()
#         elif store == 'FPT':
#             scraper = FPTScraper(urls=urls)
#             products = scraper.parse()

#         if len(products) == 0:
#            continue
#         # 6. Lưu trữ dữ liệu dưới dạng file csv "D:\\dw\\data\\fpt" lấy từ config trong bảng control.tb_log
#         filename = os.path.join(
#             data_dir, f"{store.upper()}_{current_time}.csv")
#         api.update_filename_control.tb_log(filename)
#         # 7. Update status = LR
#         api.update_status_code(id=config['id'], status='LR')

#         # 8. lưu file csv vào folder
#         # Kiểm tra thư mục tồn tại, nếu không thì tạo mới
#         os.makedirs(data_dir, exist_ok=True)

#         writer.write_to_csv(products, filename)
#         scraper.close()

# 8. Tiếp tục config
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawler data for Laptop')
    parser.add_argument('--store', type=str,
                        help='Cửa hàng mà bạn muốn craw data', required=True)

    args = parser.parse_args()
    store = args.store
    scraper = None
    sitemap_url = None
    urls = None
    if store == 'pv':
        sitemap_url = "https://phongvu.vn/sitemap_collection_products_4-laptop.xml"
        sitemap_crawler = Crawler(sitemap_url=sitemap_url)
        urls = sitemap_crawler.get_urls()

        scraper = PhongVuScraper(urls=urls)
        products = scraper.parse()
    elif store == 'fpt':
        sitemap_url = "https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml"
        sitemap_crawler = Crawler(sitemap_url=sitemap_url)
        urls = sitemap_crawler.urls

        scraper = FPTScraper(urls=urls)
        products = scraper.parse()

        # lấy những url lỗi
        products_error = []
        if len(scraper.URL_SERE) > 0:
            print(f"===> Parsing url ERROR {len(scraper.URL_SERE)}")
            for url in scraper.URL_SERE:
               # Remove url error from products
               # 1. Lấy product có url lỗi trong products
                product = next(
                    (p for p in products if p['source'] == url), None)
                if product is not None:
                    # 2. Xóa product có url lỗi ra khỏi products
                    products.remove(product)
                    print(f"Removed product from products: {product['name']}")

                # Lưu lại url lỗi để sửa sau
                products_error.append(scraper.get_product_info(url))

        products.extend(products_error)

    writer = CSVWriter()
    # write file name contain
    data_dir = None
    if store == 'fpt':
        data_dir = "D:\\dw\\data\\fpt"
    elif store == 'pv':
        data_dir = "D:\\dw\\data\\pv"

    current_time = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = os.path.join(data_dir, f"{store.upper()}_{current_time}.csv")

    # Kiểm tra thư mục tồn tại, nếu không thì tạo mới
    os.makedirs(data_dir, exist_ok=True)

    writer.write_to_csv(products, filename)
    scraper.close()
