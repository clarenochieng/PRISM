import pandas as pd
from tqdm import tqdm

from src.collectors.options_collector import OptionsCollector
from src.scrapers.sec_scraper import SECScraper
from src.utils.data_storage import DataStorage
from src.utils.logger import get_logger
from src.utils.manifest_manager import load_manifest, update_ticker_status

log = get_logger(__name__)


class DataPipeline:
    def __init__(self):
        self.manifest = load_manifest()
        self.sec_scraper = SECScraper()
        self.options_collector = OptionsCollector()
        self.storage = DataStorage()

    def run(self, tickers=None, limit_per_ticker=5):
        if tickers is None:
            tickers = list(self.manifest["tickers"].keys())

        log.info("Starting pipeline for %d ticker(s).", len(tickers))

        for ticker in tqdm(tickers, desc="Processing Tickers"):
            log.info("--- Processing %s ---", ticker)
            try:
                filings = self.sec_scraper.get_8k_filings(
                    ticker, count=limit_per_ticker
                )
                if not filings:
                    log.warning(
                        "No 8-K filings found for %s — skipping.", ticker
                    )
                    update_ticker_status(
                        ticker, "N/A", "Skipped", error="No 8-K filings found"
                    )
                    continue

                transcript_records = []
                options_records = []

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

                        options_data = self.options_collector.get_options_data(
                            ticker, date_str
                        )
                        if options_data:
                            for exp, metrics in options_data[
                                "metrics"
                            ].items():
                                options_records.append(
                                    {
                                        "ticker": ticker,
                                        "earnings_date": date_str,
                                        "year": year_str,
                                        "expiry": exp,
                                        "volume": metrics["volume"],
                                        "put_call_ratio": metrics[
                                            "put_call_ratio"
                                        ],
                                        "implied_volatility": metrics[
                                            "implied_volatility"
                                        ],
                                    }
                                )
                        else:
                            log.warning(
                                "No options data for %s on %s.",
                                ticker,
                                date_str,
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

                if options_records:
                    self.storage.save_options(pd.DataFrame(options_records))
                else:
                    log.warning("No options records collected for %s.", ticker)

                update_ticker_status(ticker, "All", "Completed")

            except Exception as e:
                log.exception("Unhandled error processing %s: %s", ticker, e)
                update_ticker_status(ticker, "All", "Failed", error=str(e))

        log.info("Pipeline complete.")


if __name__ == "__main__":
    pipeline = DataPipeline()
    test_tickers = ["AAPL", "MSFT"]
    pipeline.run(tickers=test_tickers, limit_per_ticker=2)
