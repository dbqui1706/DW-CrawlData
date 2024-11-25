
import requests
from xml.etree import ElementTree as ET
from tqdm import tqdm


class Crawler:
    def __init__(self, sitemap_url=None):
        self.sitemap_url = sitemap_url
        self.urls = []
        if self.sitemap_url is not None:
            self.fetch_sitemap()

    def fetch_sitemap(self):
        try:
            print(f"Fetching sitemap: {self.sitemap_url}")
            response = requests.get(self.sitemap_url)
            response.raise_for_status()  # Check if the request was successful
            root = ET.fromstring(response.content)
            # Extract all <loc> elements that contain the URLs
            roots = root.iter(
                '{http://www.sitemaps.org/schemas/sitemap/0.9}url')
            for url_element in tqdm(root, desc="Fetching"):
                url = url_element.find(
                    '{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                self.urls.append(url)

            print("Fetching sitemap SUCCESSED")
        except Exception as e:
            print(
                f"===> [ERROR] Error fetching or parsing {self.sitemap_url}: {e}")

    def get_urls(self):
        return self.urls[1:]
