from selenium.webdriver.common.by import By
from .base import BaseScraper
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


class PhongVuScraper(BaseScraper):

    def __init__(self, urls=None):
        super().__init__(urls)

    def get_product_info(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(lambda driver: driver.execute_script(
                "return document.readyState") == "complete")

            product = {}
            product['Name'] = self._get_element('css-nlaxuc').text
            sku_div = self._get_element('css-1f5a6jh')
            product['SKU'] = sku_div.text.split('SKU:')[-1].strip()

            product['Image'] = self._get_element(
                'css-j4683g img').get_attribute('src')
            product['Latest_Price'] = self._get_element(
                'att-product-detail-latest-price').text

            retail_price_element = self._get_element(
                'att-product-detail-retail-price')
            sale_element = self._get_element('css-2rwx6s')

            if retail_price_element and sale_element:
                product['Retail_Price'] = retail_price_element.text
                product['Sale'] = sale_element.text.replace('-', '')
            else:
                product['Retail_Price'] = None
                product['Sale'] = None

            # Scroll down and click to open the modal
            self.driver.execute_script("window.scrollBy(0, 2000)", "")
            detail_info_product = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//*[@id='__next']/div/div/div/div/div[5]/div[2]/div/div[4]/div/div[2]/div[3]/div"))
            )
            self.scroll_and_click(detail_info_product)

            # Wait for modal to display and fetch info
            modal_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.css-9s7q9u"))
            )
            info_elements = modal_element.find_elements(
                By.CSS_SELECTOR, "div.css-1i3ajxp")

            for info in info_elements:
                try:
                    label = info.find_element(
                        By.CSS_SELECTOR, "div[style*='flex: 2 1 0%']").text
                    value = info.find_element(
                        By.CSS_SELECTOR, "div[style*='flex: 3 1 0%']").text
                    product[label] = value
                except Exception as e:
                    print(f"[ERROR] Lỗi khi lấy thông tin từ modal: {e}")
                    continue

            return product

        except Exception as e:
            print(
                f"===> [ERROR] Đã xảy ra lỗi khi lấy thông tin sản phẩm từ `{url}`: {e}")
            return None
