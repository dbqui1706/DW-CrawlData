from .base import Base


class CellPhonesScraper(Base):
    def __init__(self, urls=None):
        super().__init__(urls)

    def filter_url(self, url):
        if url.startswith('https://cellphones.com.vn/macbook-'):
            return True
        return url.startswith('https://cellphones.com.vn/laptop-')

    def get_product_info(self, url):
        try:
            if self.filter_url(url) is not True:
                return None
            self.driver.get(url)

        except Exception as e:
            print(f"An error occurred while fetching product information: {e}")
