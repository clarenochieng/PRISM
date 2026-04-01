import requests
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry

from src.utils.logger import get_logger

SEC_RATE_LIMIT = 10
log = get_logger(__name__)


class SECScraper:
    def __init__(
        self, user_agent="PRISM-Data-Bot/1.0 (contact: your-email@example.com)"
    ):
        self.headers = {"User-Agent": user_agent}
        self.base_url = "https://www.sec.gov/cgi-bin/browse-edgar"

    @sleep_and_retry
    @limits(calls=SEC_RATE_LIMIT, period=1)
    def _make_request(self, url, params=None):
        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()
        return response

    def _fetch_page(self, ticker, start, page_size):
        params = {
            "action": "getcompany",
            "CIK": ticker,
            "type": "8-K",
            "output": "atom",
            "count": page_size,
            "start": start,
        }
        response = self._make_request(self.base_url, params=params)
        soup = BeautifulSoup(response.content, "xml")
        entries = soup.find_all("entry")
        page = []
        for entry in entries:
            page.append(
                {
                    "ticker": ticker,
                    "type": "8-K",
                    "date": entry.find("updated").text[:10],
                    "link": entry.find("link")["href"].replace(
                        "-index.htm", ".txt"
                    ),
                }
            )
        return page

    def get_8k_filings(self, ticker, count=100):
        PAGE_SIZE = 100
        all_filings = []
        start = 0
        try:
            while len(all_filings) < count:
                page = self._fetch_page(ticker, start, PAGE_SIZE)
                if not page:
                    break
                all_filings.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                start += PAGE_SIZE
            all_filings = all_filings[:count]
            log.info(
                "Found %d 8-K filings for %s.", len(all_filings), ticker
            )
            return all_filings
        except Exception as e:
            log.error("Failed to fetch 8-K filings for %s: %s", ticker, e)
            return []

    def extract_transcript(self, filing_url):
        try:
            response = self._make_request(filing_url)
            log.debug("Extracted transcript from %s.", filing_url)
            return response.text[:5000]
        except Exception as e:
            log.error(
                "Failed to extract transcript from %s: %s", filing_url, e
            )
            return None


if __name__ == "__main__":
    scraper = SECScraper()
    filings = scraper.get_8k_filings("AAPL", count=5)
    if filings:
        log.info("Sample filing link: %s", filings[0]["link"])
