import json
import os
import ssl
from datetime import datetime

import pandas as pd
import requests

from src.utils.logger import get_logger

ssl._create_default_https_context = ssl._create_unverified_context

MANIFEST_PATH = "manifest.json"
log = get_logger(__name__)


def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "PRISM-Data-Bot/1.0 (contact: your-email@example.com)"
    }
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        df = tables[0]
        df["Symbol"] = df["Symbol"].str.replace(".", "-", regex=False)
        tickers = (
            df[["Symbol", "Security"]]
            .rename(columns={"Symbol": "ticker", "Security": "name"})
            .to_dict("records")
        )
        log.info("Fetched %d tickers from Wikipedia.", len(tickers))
        return tickers
    except Exception as e:
        log.error(
            "Failed to fetch S&P 500 tickers: %s. Using fallback list.", e
        )
        return [
            {"ticker": "AAPL", "name": "Apple Inc."},
            {"ticker": "MSFT", "name": "Microsoft Corp."},
            {"ticker": "GOOGL", "name": "Alphabet Inc."},
            {"ticker": "AMZN", "name": "Amazon.com Inc."},
            {"ticker": "TSLA", "name": "Tesla Inc."},
        ]


def load_manifest():
    if os.path.exists(MANIFEST_PATH) and os.path.getsize(MANIFEST_PATH) > 0:
        with open(MANIFEST_PATH, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                log.warning(
                    "%s was corrupted — resetting manifest.", MANIFEST_PATH
                )
    return {"last_updated": None, "tickers": {}}


def save_manifest(manifest):
    manifest["last_updated"] = datetime.now().isoformat()
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=4)
    log.debug("Manifest saved to %s.", MANIFEST_PATH)


def update_ticker_status(ticker, year, status, error=None):
    manifest = load_manifest()
    if ticker not in manifest["tickers"]:
        manifest["tickers"][ticker] = {}

    manifest["tickers"][ticker][str(year)] = {
        "status": status,
        "last_attempt": datetime.now().isoformat(),
        "error": error,
    }
    if error:
        log.warning("Ticker %s [%s] — %s: %s", ticker, year, status, error)
    else:
        log.info("Ticker %s [%s] — %s.", ticker, year, status)
    save_manifest(manifest)


if __name__ == "__main__":
    try:
        tickers = get_sp500_tickers()
        manifest = load_manifest()

        for t in tickers:
            if t["ticker"] not in manifest["tickers"]:
                manifest["tickers"][t["ticker"]] = {
                    "name": t["name"],
                    "history": {},
                }

        save_manifest(manifest)
        log.info("Initialized manifest with %d tickers.", len(tickers))
    except Exception as e:
        log.exception("Fatal error in manifest initialization: %s", e)
