from .base import BaseScraper
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import StaleElementReferenceException


class FPTScraper(BaseScraper):
    categories = ['dell', 'asus', 'hp', 'macbook', 'acer',
                  'msi', 'lenovo', 'gigabyte', 'hwawei', 'lg', 'masstel']

    def __init__(self, urls=None):
        super().__init__(urls)
        self.URL_SERE = []

    def filter_url(self, url):
        if url.startswith('https://cellphones.com.vn/macbook-'):
            return True
        return url.startswith('https://cellphones.com.vn/laptop-')

    def get_product_info(self, url):
        product = {}
        # Use a function to fetch elements with retry on StaleElementReferenceException
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(lambda driver: driver.execute_script(
                "return document.readyState") == "complete")
            product = {}
            id_element = self._get_element(
                '//div[@class="w-fit"]/span', by=By.XPATH)
            product['id'] = id_element.text if id_element else ""

            product['source'] = url

            h1_element = self._get_element(
                '//h1[contains(@class, "text-textOnWhitePrimary")]', by=By.XPATH)

            product['name'] = h1_element.text if h1_element else ""

            product['brand'] = ""
            for word in h1_element.text.lower().split(' '):
                if word in self.categories:
                    product['brand'] = word.capitalize()
                    break

            img_src = self._get_element(
                'swiper-zoom-container').find_element(By.TAG_NAME, 'img').get_attribute('src')
            product['img-src'] = img_src if img_src else ""

            # x_path_available = '//div[contains(@class, "flex items-center justify-between bg-yellow-yellow-2")]//p[@class="text-yellow-yellow-8 h6-semibold"]'
            # available_element = self._get_element(
            #     x_path_available, By.XPATH)
            # product['Available'] = False if available_element is not None else True

            latest_price = self._get_element(
                '//*[@id="tradePrice"]/div[1]/div[1]/span[2]', By.XPATH)
            product['latest-price'] = latest_price.text.replace(
                "₫", "").strip() if latest_price is not None else "0"

            retail_price = self._get_element(
                '//*[@id="tradePrice"]/div[1]/div[1]/div[1]/span[1]', By.XPATH)
            if retail_price is not None:
                if 'Điểm thưởng' in retail_price.text.strip():
                    product['retail-price'] = "0"
                else:
                    product['retail-price'] = retail_price.text.replace(
                        "₫", "").strip() if retail_price is not None else "0"
            else:
                product['retail-price'] = "0"

            sale = self._get_element(
                '//*[@id="tradePrice"]/div[1]/div[1]/div[1]/span[2]', By.XPATH)
            if sale is not None:
                product['sale'] = sale.text.replace("%", "").strip()
            else:
                product['sale'] = "0"

            stop_bussines = self._get_element(
                "//p[contains(@class, 'absolute') and contains(@class, 'left-4') and contains(@class, '-tracking-[.32px]') and contains(@class, 'text-textOnWhitePrimary') and contains(@class, 'h5-16-semibold')]", By.XPATH)
            out_product = self._get_element('text-yellow-yellow-8')
            product['available'] = False if (
                stop_bussines or out_product) is not None else True
            # product['Trả góp'] = self._get_element(
            #     '//*[@id="tradePrice"]/div[1]/div[3]/span[2]', By.XPATH).text

            # # Scroll down and click to open the modal
            # self.driver.execute_script("window.scrollBy(0, 150)", "")
            # detail_info_product = WebDriverWait(self.driver, 20).until(
            #     EC.element_to_be_clickable(
            #         (By.XPATH, "/html/body/main/div[2]/section[2]/div[1]/div[2]/div[1]/button"))
            # )
            # self.scroll_and_click(detail_info_product)

            # # Get product specifications
            # modal_element = WebDriverWait(self.driver, 5).until(
            #     EC.presence_of_element_located(
            #         (By.CLASS_NAME, "Sheet_body__VKc95"))
            # )

            # spec_items = modal_element.find_element(By.CSS_SELECTOR,
            #                                         '#drawer-container-body > div.px-5.pb-15')

            # # Lấy tất cả các phần tử có id bắt đầu bằng "spec-item-"
            # specs_index = spec_items.find_elements(
            #     By.CSS_SELECTOR, "[id^='spec-item-']")

            # # Duyệt qua từng spec_item
            # for spec_item in specs_index:
            #     rows = spec_item.find_elements(By.CSS_SELECTOR, '.flex.gap-2')
            #     for row in rows:
            #         try:
            #             label = row.find_element(
            #                 By.CSS_SELECTOR, '.w-2\\/5').text
            #             value = row.find_element(
            #                 By.CSS_SELECTOR, '.flex-1').text
            #             product[label] = value
            #         except Exception as e:
            #             print(
            #                 f"An error occurred while fetching label and value: {e}")
            return product
        except Exception as e:
            print(f"An error occurred while fetching product information: {e}")
            self.URL_SERE.append(url)
            return {
                'id': '',
                'source': url,
                'name': '',
                'brand': '',
                'img-src': '',
                'latest-price': 0,
                'retail-price': 0,
                'sale': 0,
                'available': False,
            }
