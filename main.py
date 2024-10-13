from crawler import Crawler
from scraper.phongvu import PhongVuScraper
from writer.csv_writer import CSVWriter
import datetime


if __name__ == '__main__':
    sitemap_url = "https://phongvu.vn/sitemap_collection_products_4-laptop.xml"
    sitemap_crawler = Crawler(sitemap_url=sitemap_url)
    urls = sitemap_crawler.get_urls()

    scraper = PhongVuScraper(urls=urls)
    products = scraper.parse()
    # print(products)

    writer = CSVWriter()
    # write file name contain

    writer.write_to_csv(
        products, f"phongvu_{datetime.datetime.now()}.csv")
    scraper.close()
