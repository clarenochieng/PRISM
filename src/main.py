import pandas as pd
from tqdm import tqdm

from src.scrapers.sec_scraper import SECScraper
from src.utils.data_storage import DataStorage
from src.utils.logger import get_logger
from src.utils.manifest_manager import load_manifest, update_ticker_status

log = get_logger(__name__)


class DataPipeline:
    def __init__(self):
        self.manifest = load_manifest()
        self.sec_scraper = SECScraper()
        self.storage = DataStorage()

    def _already_completed(self, ticker):
        entry = self.manifest.get("tickers", {}).get(ticker, {})
        return entry.get("All", {}).get("status") == "Completed"

    def run(self, tickers=None, limit_per_ticker=100, resume=True):
        if tickers is None:
            tickers = list(self.manifest["tickers"].keys())

        pending = (
            [t for t in tickers if not self._already_completed(t)]
            if resume
            else tickers
        )
        skipped = len(tickers) - len(pending)
        if skipped:
            log.info(
                "Resuming — skipping %d already-completed ticker(s).", skipped
            )

        log.info(
            "Starting pipeline for %d ticker(s) (limit=%d).",
            len(pending),
            limit_per_ticker,
        )

        for ticker in tqdm(pending, desc="Collecting"):
            log.info("--- Processing %s ---", ticker)
            try:
                filings = self.sec_scraper.get_8k_filings(
                    ticker, count=limit_per_ticker
                )
                if not filings:
                    log.warning(
                        "No 8-K filings for %s — skipping.", ticker
                    )
                    update_ticker_status(
                        ticker, "N/A", "Skipped", error="No 8-K filings found"
                    )
                    continue

                transcript_records = []

                for filing in filings:
                    date_str = filing["date"]
                    year_str = date_str[:4]

                    transcript_text = self.sec_scraper.extract_transcript(
                        filing["link"]
                    )
                    if transcript_text:
                        transcript_records.append(
                            {
                                "ticker": ticker,
                                "company_name": self.manifest["tickers"][
                                    ticker
                                ].get("name", "Unknown"),
                                "earnings_date": date_str,
                                "fiscal_quarter": "N/A",
                                "fiscal_year": int(year_str),
                                "year": year_str,
                                "raw_transcript": transcript_text,
                            }
                        )
                    else:
                        log.warning(
                            "Empty transcript for %s filing %s.",
                            ticker,
                            filing["link"],
                        )

                if transcript_records:
                    self.storage.save_transcripts(
                        pd.DataFrame(transcript_records)
                    )
                else:
                    log.warning(
                        "No transcript records collected for %s.", ticker
                    )

                update_ticker_status(ticker, "All", "Completed")

            except Exception as e:
                log.exception(
                    "Unhandled error processing %s: %s", ticker, e
                )
                update_ticker_status(ticker, "All", "Failed", error=str(e))

        log.info("Pipeline complete.")


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run(limit_per_ticker=100, resume=True)
