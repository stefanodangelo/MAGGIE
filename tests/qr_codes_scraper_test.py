import sys
sys.path.append('./src')
sys.path.append('./src/MAGGIE')
from MAGGIE.preprocessing import QRCodeScraper
from MAGGIE.utils import BASE_URL

def test_scraper():
    scraper = QRCodeScraper()
    scraped_data = scraper.scrape([BASE_URL+"Code_number_designations_BRO_BPW_en_2020_39342001.pdf"])
    assert scraped_data is not None