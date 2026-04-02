import re

import requests
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry

from src.utils.logger import get_logger

EDGAR_SEARCH = "https://www.sec.gov/cgi-bin/browse-edgar"
MAX_CHARS = 100_000
SEC_RATE_LIMIT = 10

DOCUMENT_RE = re.compile(
    r"<DOCUMENT>(.*?)</DOCUMENT>", re.DOTALL | re.IGNORECASE
)
TYPE_RE = re.compile(r"<TYPE>([^\n<]+)", re.IGNORECASE)
TEXT_RE = re.compile(r"<TEXT>(.*?)</TEXT>", re.DOTALL | re.IGNORECASE)

PREFERRED_TYPES = ("EX-99.1", "EX-99", "8-K", "8-K/A")

log = get_logger(__name__)


class SECScraper:
    def __init__(
        self,
        user_agent="PRISM-Data-Bot/1.0 (contact: your-email@example.com)",
    ):
        self.headers = {"User-Agent": user_agent}

    @sleep_and_retry
    @limits(calls=SEC_RATE_LIMIT, period=1)
    def _make_request(self, url, params=None):
        response = requests.get(
            url, params=params, headers=self.headers, timeout=30
        )
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
        response = self._make_request(EDGAR_SEARCH, params=params)
        soup = BeautifulSoup(response.content, "xml")
        page = []
        for entry in soup.find_all("entry"):
            page.append(
                {
                    "ticker": ticker,
                    "type": "8-K",
                    "date": entry.find("updated").text[:10],
                    "link": entry.find("link")["href"],
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

    def _index_to_txt(self, url):
        """Convert an -index.htm URL to its full submission .txt URL."""
        if url.endswith("-index.htm"):
            return url.replace("-index.htm", ".txt")
        return url

    def _parse_documents(self, submission_text):
        """
        Parse the EDGAR full submission text and return a dict mapping
        document type -> clean plain text (HTML stripped).
        Processes only PREFERRED_TYPES to avoid parsing XBRL/XML blobs.
        """
        docs = {}
        for block_match in DOCUMENT_RE.finditer(submission_text):
            block = block_match.group(1)
            type_match = TYPE_RE.search(block)
            if not type_match:
                continue
            doc_type = type_match.group(1).strip()
            if doc_type not in PREFERRED_TYPES:
                continue
            text_match = TEXT_RE.search(block)
            if not text_match:
                continue
            raw_html = text_match.group(1)
            soup = BeautifulSoup(raw_html, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            clean = soup.get_text(separator="\n", strip=True)
            if len(clean) > 200:
                docs[doc_type] = clean
        return docs

    def extract_transcript(self, index_url):
        """
        Fetch the full EDGAR submission .txt, extract the best available
        document (EX-99.1 > EX-99 > 8-K > 8-K/A), and return up to
        MAX_CHARS of clean text.
        """
        try:
            txt_url = self._index_to_txt(index_url)
            response = self._make_request(txt_url)
            docs = self._parse_documents(response.text)

            for preferred in PREFERRED_TYPES:
                if preferred in docs:
                    text = docs[preferred]
                    log.debug(
                        "Extracted %d chars (%s) from %s.",
                        len(text),
                        preferred,
                        txt_url,
                    )
                    return text[:MAX_CHARS]

            log.warning(
                "No usable document found in %s — raw fallback.", txt_url
            )
            return response.text[:MAX_CHARS]

        except Exception as e:
            log.error(
                "Failed to extract transcript from %s: %s", index_url, e
            )
            return None


if __name__ == "__main__":
    scraper = SECScraper()
    filings = scraper.get_8k_filings("AAPL", count=3)
    for f in filings:
        text = scraper.extract_transcript(f["link"])
        chars = len(text) if text else 0
        preview = (text or "")[:300].replace("\n", " ")
        print(f"  {f['date']}  {chars:,} chars")
        print(f"  {preview}")
        print()
