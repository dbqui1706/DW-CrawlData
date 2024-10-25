from crawler import Crawler
from scraper.phongvu import PhongVuScraper
from scraper.fpt import FPTScraper
from writer.csv_writer import CSVWriter
import datetime
import argparse
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawler data for Laptop')
    parser.add_argument('--store', type=str,
                        help='Cửa hàng mà bạn muốn craw data', required=True)

    args = parser.parse_args()
    store = args.store
    if store == 'phongvu':
        sitemap_url = "https://phongvu.vn/sitemap_collection_products_4-laptop.xml"
        sitemap_crawler = Crawler(sitemap_url=sitemap_url)
        urls = sitemap_crawler.get_urls()

        scraper = PhongVuScraper(urls=urls)
        products = scraper.parse()
    elif store == 'fpt':
        sitemap_url = "https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml"
        sitemap_crawler = Crawler(sitemap_url=sitemap_url)
        urls = sitemap_crawler.urls

        cellphones = FPTScraper(urls=urls)
        products = cellphones.parse()

    writer = CSVWriter()
    # write file name contain
    current_dir = os.getcwd()  # Thư mục hiện tại
    data_dir = os.path.join(current_dir, "data")  # Thư mục con "data"
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(data_dir, f"{store}_{current_time}.csv")

    # Kiểm tra thư mục tồn tại, nếu không thì tạo mới
    os.makedirs(data_dir, exist_ok=True)

    writer.write_to_csv(products, filename)
    scraper.close()
