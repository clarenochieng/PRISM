import re

import pandas as pd

from src.utils.logger import get_logger

log = get_logger(__name__)

TRANSCRIPTS_PATH = "data/transcripts"
EARNINGS_PATTERNS = re.compile(
    r"item\s+2\.02"
    r"|results of operations"
    r"|quarterly.*results"
    r"|revenue.*quarter"
    r"|earnings per share",
    re.IGNORECASE,
)


def load_data():
    log.info("Loading transcript parquet files from %s.", TRANSCRIPTS_PATH)
    df = pd.read_parquet(TRANSCRIPTS_PATH, engine="pyarrow")
    df["earnings_date"] = pd.to_datetime(df["earnings_date"])
    df["char_count"] = df["raw_transcript"].str.len()
    df["is_earnings"] = df["raw_transcript"].str.contains(
        EARNINGS_PATTERNS, na=False
    )
    log.info(
        "Loaded %d rows across %d tickers.",
        len(df),
        df["ticker"].nunique(),
    )
    return df


def section(title):
    print()
    print("=" * 55)
    print(f"  {title}")
    print("=" * 55)


def report_overview(df):
    section("OVERVIEW")
    print(f"  Total rows:          {len(df):,}")
    print(f"  Unique tickers:      {df['ticker'].nunique()}")
    print(f"  Avg filings/ticker:  {len(df) / df['ticker'].nunique():.1f}")
    print(f"  Null rows:           {df.isnull().any(axis=1).sum()}")


def report_date_range(df):
    section("DATE RANGE")
    earliest = df["earnings_date"].min().date()
    latest = df["earnings_date"].max().date()
    span = (df["earnings_date"].max() - df["earnings_date"].min()).days // 365
    print(f"  Earliest filing:  {earliest}")
    print(f"  Latest filing:    {latest}")
    print(f"  History span:     {span} years")


def report_transcript_length(df):
    section("RAW TRANSCRIPT LENGTH (chars)")
    stats = df["char_count"].describe()
    print(f"  Min:     {stats['min']:,.0f}")
    print(f"  25th %:  {stats['25%']:,.0f}")
    print(f"  Median:  {stats['50%']:,.0f}")
    print(f"  Mean:    {stats['mean']:,.0f}")
    print(f"  75th %:  {stats['75%']:,.0f}")
    print(f"  Max:     {stats['max']:,.0f}")
    truncated = (df["char_count"] >= 4990).sum()
    pct = truncated / len(df) * 100
    print(f"\n  Truncated at 5000 chars: {truncated:,} rows ({pct:.1f}%)")
    if pct > 90:
        print("  WARNING: nearly all transcripts are hitting the 5000-char")
        print("  cap — earnings body text is likely being cut off.")


def report_earnings_detection(df):
    section("EARNINGS FILING DETECTION")
    total = len(df)
    count = df["is_earnings"].sum()
    pct = count / total * 100
    print(f"  Earnings-related:    {count:,} / {total:,} ({pct:.1f}%)")
    projected_500 = int(count / df["ticker"].nunique() * 498)
    print(f"  Projected @ 498 tickers: ~{projected_500:,} earnings rows")
    shortfall = max(0, 50000 - projected_500)
    if shortfall:
        print(f"  Shortfall vs 50k target: ~{shortfall:,} rows")
        print("  -> S&P 400 / S&P 600 expansion needed.")
    else:
        print("  -> 50,000 row target is on track.")


def report_yearly_coverage(df):
    section("FILINGS PER YEAR")
    by_year = (
        df.groupby("fiscal_year")
        .agg(filings=("ticker", "count"), tickers=("ticker", "nunique"))
        .reset_index()
    )
    print(f"  {'Year':<8} {'Filings':>8} {'Tickers':>8}")
    print(f"  {'-'*8} {'-'*8} {'-'*8}")
    for _, row in by_year.iterrows():
        print(
            f"  {int(row['fiscal_year']):<8} "
            f"{int(row['filings']):>8,} "
            f"{int(row['tickers']):>8}"
        )


def report_ticker_coverage(df):
    section("TICKER FILING COUNTS (sample)")
    counts = df["ticker"].value_counts()
    under = (counts < 100).sum()
    exact = (counts == 100).sum()
    print(f"  Tickers with exactly 100 filings: {exact}")
    print(f"  Tickers with fewer than 100:      {under}")
    print()
    print("  Bottom 10 (fewest filings):")
    print(f"  {'Ticker':<10} {'Count':>6}")
    print(f"  {'-'*10} {'-'*6}")
    for ticker, cnt in counts.tail(10).items():
        print(f"  {ticker:<10} {cnt:>6}")


def report_schema(df):
    section("SCHEMA & NULL CHECK")
    print(f"  {'Column':<20} {'Dtype':<12} {'Nulls':>6}")
    print(f"  {'-'*20} {'-'*12} {'-'*6}")
    for col in df.columns:
        print(
            f"  {col:<20} {str(df[col].dtype):<12} "
            f"{int(df[col].isnull().sum()):>6}"
        )


def run_eda():
    df = load_data()
    report_overview(df)
    report_date_range(df)
    report_transcript_length(df)
    report_earnings_detection(df)
    report_yearly_coverage(df)
    report_ticker_coverage(df)
    report_schema(df)
    print()
    log.info("EDA complete.")


if __name__ == "__main__":
    run_eda()
