import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from src.utils.logger import get_logger

log = get_logger(__name__)

RETURN_WINDOWS = [1, 3, 5, 10]
HISTORY_YEARS = 12
REQUEST_DELAY = 0.5


def _fetch_prices(ticker, start, end):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            log.warning("No price data returned for %s.", ticker)
            return None
        df.index = pd.to_datetime(df.index).tz_localize(None)
        return df[["Close"]]
    except Exception as e:
        log.error("Price fetch failed for %s: %s", ticker, e)
        return None


def compute_forward_returns(prices, earnings_date):
    try:
        ed = pd.Timestamp(earnings_date)
        future_prices = prices[prices.index > ed]

        if future_prices.empty:
            return {}

        base_idx = prices.index.searchsorted(ed, side="right")
        if base_idx >= len(prices):
            return {}

        base_price = prices.iloc[base_idx]["Close"]
        results = {}

        for window in RETURN_WINDOWS:
            target_idx = base_idx + window
            if target_idx < len(prices):
                fwd_price = prices.iloc[target_idx]["Close"]
                ret = (fwd_price - base_price) / base_price
                results[f"return_{window}d"] = round(float(ret), 6)
            else:
                results[f"return_{window}d"] = None

        return results
    except Exception as e:
        log.error(
            "Forward return computation failed for %s: %s", earnings_date, e
        )
        return {}


def compute_realized_volatility(prices, earnings_date):
    try:
        ed = pd.Timestamp(earnings_date)
        results = {}

        for window in RETURN_WINDOWS:
            end_date = ed + timedelta(days=window * 2)
            window_prices = prices[
                (prices.index > ed) & (prices.index <= end_date)
            ].head(window)

            if len(window_prices) < 2:
                results[f"realized_vol_{window}d"] = None
                continue

            log_returns = window_prices["Close"].pct_change().dropna()
            vol = float(log_returns.std() * (252 ** 0.5))
            results[f"realized_vol_{window}d"] = round(vol, 6)

        return results
    except Exception as e:
        log.error(
            "Realized vol computation failed for %s: %s", earnings_date, e
        )
        return {}


def collect_price_features(ticker, earnings_dates):
    start = (
        datetime.now() - timedelta(days=365 * HISTORY_YEARS)
    ).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    log.debug("Fetching %s years of prices for %s.", HISTORY_YEARS, ticker)
    prices = _fetch_prices(ticker, start, end)
    time.sleep(REQUEST_DELAY)

    if prices is None:
        return []

    records = []
    for ed in earnings_dates:
        fwd = compute_forward_returns(prices, ed)
        vol = compute_realized_volatility(prices, ed)
        if fwd:
            record = {"ticker": ticker, "earnings_date": ed}
            record.update(fwd)
            record.update(vol)
            records.append(record)
        else:
            log.debug(
                "No forward price data for %s on %s — outside history window.",
                ticker,
                ed,
            )

    return records
