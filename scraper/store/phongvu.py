from selenium.webdriver.common.by import By
from .base import BaseScraper
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class PhongVuScraper(BaseScraper):

    def __init__(self, urls=None):
        super().__init__(urls)

    def get_product_info(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(lambda driver: driver.execute_script(
                "return document.readyState") == "complete")

            product = {}
            sku_div = self._get_element('css-1f5a6jh')
            product['id'] = "sku." + sku_div.text.split('SKU:')[-1].strip()
            product['store'] = "PV"
            product['source'] = url

            product['name'] = self._get_element('css-nlaxuc').text

            product['brand'] = sku_div.text.split(
                'SKU:')[0].split('Thương hiệu ')[-1].strip()

            product['img-src'] = self._get_element(
                'css-j4683g img').get_attribute('src')

            product['latest-price'] = self._get_element(
                'att-product-detail-latest-price').text.replace(
                    "₫", "").strip()

            retail_price_element = self._get_element(
                'att-product-detail-retail-price')
            sale_element = self._get_element('css-2rwx6s')

            if retail_price_element and sale_element:
                product['retail-price'] = retail_price_element.text.replace(
                    "₫", "").strip()

                product['sale'] = sale_element.text.replace(
                    '-', '').replace('%', '')
            else:
                product['retail-price'] = '0'

                product['sale'] = '0'

            available = self._get_element('css-fdtrln')
            if available:
                text = available.text.lower()
                product['available'] = True if "mua ngay" in text else False
            else:
                product['available'] = False

            return product

        except Exception as e:
            print(
                f"===> [ERROR] Đã xảy ra lỗi khi lấy thông tin sản phẩm từ `{url}`: {e}")
            return {
                'id': '',
                'store': 'PV',
                'source': url,
                'name': '',
                'brand': '',
                'img-src': '',
                'latest-price': 0,
                'retail-price': 0,
                'sale': 0,
                'available': False,
            }
