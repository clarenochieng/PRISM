import json
from datetime import datetime, timedelta

import yfinance as yf

from src.utils.logger import get_logger

log = get_logger(__name__)


class OptionsCollector:
    def __init__(self):
        pass

    def get_options_data(self, ticker, earnings_date):
        try:
            stock = yf.Ticker(ticker)
            expirations = stock.options
            if not expirations:
                log.warning("No option expirations available for %s.", ticker)
                return None

            if isinstance(earnings_date, str):
                earnings_date = datetime.strptime(earnings_date, "%Y-%m-%d")

            exp_dates = [datetime.strptime(e, "%Y-%m-%d") for e in expirations]
            before = [e for e in exp_dates if e < earnings_date]
            after = [e for e in exp_dates if e >= earnings_date]

            if not before or not after:
                log.warning(
                    "No bracketing expiries found for %s on %s"
                    " — skipping options.",
                    ticker,
                    earnings_date.strftime("%Y-%m-%d"),
                )
                return None

            nearest_before = max(before).strftime("%Y-%m-%d")
            nearest_after = min(after).strftime("%Y-%m-%d")

            log.debug(
                "Options for %s: before=%s, after=%s.",
                ticker,
                nearest_before,
                nearest_after,
            )

            data = {
                "ticker": ticker,
                "earnings_date": earnings_date.strftime("%Y-%m-%d"),
                "before_expiry": nearest_before,
                "after_expiry": nearest_after,
                "metrics": {},
            }

            for exp in [nearest_before, nearest_after]:
                opt_chain = stock.option_chain(exp)
                calls = opt_chain.calls
                puts = opt_chain.puts

                total_vol = calls["volume"].sum() + puts["volume"].sum()
                pc_ratio = (
                    puts["volume"].sum() / calls["volume"].sum()
                    if calls["volume"].sum() > 0
                    else 0
                )
                avg_iv = (
                    calls["impliedVolatility"].mean()
                    + puts["impliedVolatility"].mean()
                ) / 2

                data["metrics"][exp] = {
                    "volume": float(total_vol),
                    "put_call_ratio": float(pc_ratio),
                    "implied_volatility": float(avg_iv),
                }
                log.debug(
                    "%s expiry %s — vol=%.0f, pc=%.3f, iv=%.4f.",
                    ticker,
                    exp,
                    total_vol,
                    pc_ratio,
                    avg_iv,
                )

            return data
        except Exception as e:
            log.error(
                "Failed to collect options for %s on %s: %s",
                ticker,
                earnings_date,
                e,
            )
            return None


if __name__ == "__main__":
    collector = OptionsCollector()
    test_date = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d")
    data = collector.get_options_data("AAPL", test_date)
    if data:
        log.info("Options data for AAPL on %s:", test_date)
        print(json.dumps(data, indent=4))
    else:
        stock = yf.Ticker("AAPL")
        if stock.options:
            log.info("Available expirations: %s", stock.options[:5])
            data = collector.get_options_data("AAPL", stock.options[1])
            if data:
                print(json.dumps(data, indent=4))
        else:
            log.warning("No options data found at all for AAPL.")
