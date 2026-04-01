import json
import os
from datetime import datetime

import pandas as pd

from src.utils.logger import get_logger

log = get_logger(__name__)

RETURN_COLS = [
    "return_1d", "return_3d", "return_5d", "return_10d",
]
VOL_COLS = [
    "realized_vol_1d", "realized_vol_3d",
    "realized_vol_5d", "realized_vol_10d",
]
OPTIONS_COLS = ["options_volume", "avg_iv", "avg_pc_ratio"]
TARGET_ROW_COUNT = 20000


def _missingness(df):
    total = len(df)
    report = {}
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        report[col] = {
            "null_count": null_count,
            "null_pct": round(null_count / total * 100, 2) if total else 0,
        }
    return report


def _return_stats(df):
    stats = {}
    for col in RETURN_COLS + VOL_COLS:
        if col in df.columns:
            series = df[col].dropna()
            if series.empty:
                stats[col] = {"count": 0}
                continue
            stats[col] = {
                "count": int(series.count()),
                "mean": round(float(series.mean()), 6),
                "std": round(float(series.std()), 6),
                "min": round(float(series.min()), 6),
                "p25": round(float(series.quantile(0.25)), 6),
                "median": round(float(series.median()), 6),
                "p75": round(float(series.quantile(0.75)), 6),
                "max": round(float(series.max()), 6),
                "outliers_3std": int(
                    (
                        (series - series.mean()).abs()
                        > 3 * series.std()
                    ).sum()
                ),
            }
    return stats


def _coverage(df):
    if df.empty:
        return {}
    by_year = (
        df.copy()
        .assign(year=pd.to_datetime(df["earnings_date"]).dt.year)
        .groupby("year")
        .agg(events=("ticker", "count"), tickers=("ticker", "nunique"))
        .reset_index()
        .to_dict("records")
    )
    by_sector = {}
    if "sector" in df.columns:
        by_sector = (
            df.groupby("sector")
            .agg(events=("ticker", "count"), tickers=("ticker", "nunique"))
            .to_dict("index")
        )
    return {"by_year": by_year, "by_sector": by_sector}


def _flag_summary(df):
    if "quality_flag" not in df.columns:
        return {}
    return df["quality_flag"].value_counts().to_dict()


def _alignment_summary(df):
    if "alignment_flag" not in df.columns:
        return {}
    return df["alignment_flag"].value_counts().to_dict()


def generate_report(df, row_counts, output_path="data/quality_report.json"):
    log.info("Generating data quality report.")

    final_count = len(df)
    target_met = final_count >= TARGET_ROW_COUNT
    shortfall = max(0, TARGET_ROW_COUNT - final_count)

    remediation = []
    if not target_met:
        remediation = [
            "Run Phase 1 for all 500 S&P 500 tickers "
            "with limit_per_ticker=40.",
            "Extend history beyond 12 years (SEC EDGAR supports 15+).",
            "Include 8-K/A amended filings as additional transcript sources.",
            "Add Russell 1000 or mid-cap tickers to supplement S&P 500.",
            "Recover flagged 'missing_price_data' events via alternative "
            "price sources (e.g. Alpha Vantage, Polygon.io).",
        ]

    report = {
        "generated_at": datetime.now().isoformat(),
        "row_counts_by_stage": row_counts,
        "final_row_count": final_count,
        "target_row_count": TARGET_ROW_COUNT,
        "target_met": target_met,
        "shortfall": shortfall,
        "remediation_steps": remediation,
        "missingness": _missingness(df),
        "return_statistics": _return_stats(df),
        "coverage": _coverage(df),
        "quality_flags": _flag_summary(df),
        "alignment_flags": _alignment_summary(df),
        "tickers_covered": int(df["ticker"].nunique()) if not df.empty else 0,
        "date_range": {
            "min": str(
                pd.to_datetime(df["earnings_date"]).min()
            ) if not df.empty else None,
            "max": str(
                pd.to_datetime(df["earnings_date"]).max()
            ) if not df.empty else None,
        },
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=4, default=str)

    log.info("Quality report written to %s.", output_path)
    log.info(
        "Final row count: %d / %d target (%s).",
        final_count,
        TARGET_ROW_COUNT,
        "MET" if target_met else f"SHORTFALL of {shortfall}",
    )
    if not target_met:
        for step in remediation:
            log.warning("Remediation: %s", step)

    return report
