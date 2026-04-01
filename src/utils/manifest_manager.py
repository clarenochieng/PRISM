import json
import os
from datetime import datetime

import pandas as pd

from src.utils.logger import get_logger

MANIFEST_PATH = "manifest.json"
log = get_logger(__name__)


SP500_CSV = "data/reference/sp500_tickers.csv"


def get_sp500_tickers():
    try:
        df = pd.read_csv(SP500_CSV)
        tickers = df[["ticker", "name"]].to_dict("records")
        log.info("Loaded %d tickers from %s.", len(tickers), SP500_CSV)
        return tickers
    except Exception as e:
        log.error("Failed to load tickers from %s: %s.", SP500_CSV, e)
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
