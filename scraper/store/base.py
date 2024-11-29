from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from tqdm import tqdm
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time


class BaseScraper(ABC):
    def __init__(self, urls=None):
        self.urls = urls
        try:  # Run in headless mode
            # Sử dụng ChromeDriverManager để lấy đường dẫn đến chromedriver.exe
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service)
            print("Khởi tạo driver thành công")
        except Exception as e:
            print(f"Đã xảy ra lỗi khi khởi tạo Selenium WebDriver: {e}")

    @abstractmethod
    def get_product_info(self, url):
        """
        Get product information from the given URL.
        """
        pass

    def parse(self):
        """Parse product information from the fetched URLs."""
        products = []
        for index, url in enumerate(tqdm(self.urls, desc="Parsing products")):
            if index == 50:
                break
            product = self.get_product_info(url)
            if product is not None:
                products.append(product)
        return products

    def _get_element(self, selector, by=By.CLASS_NAME):
        """Get element by selector with specified method."""
        try:
            return WebDriverWait(self.driver, 0.5).until(
                EC.presence_of_element_located((by, selector))
            )
        except Exception as e:
            print(f"====> [WARNING] Selector `{selector}` not found")
            return None

    def scroll_and_click(self, element):
        """Scroll to an element and click it using JavaScript."""
        if element:
            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", element)
            time.sleep(1)  # Optional wait
            self.driver.execute_script("arguments[0].click();", element)

    def close(self):
        self.driver.quit()
