import pandas as pd

from src.utils.logger import get_logger

log = get_logger(__name__)


def load_transcripts(path="data/transcripts"):
    try:
        df = pd.read_parquet(path, engine="pyarrow")
        df["ticker"] = df["ticker"].astype(str)
        df["earnings_date"] = pd.to_datetime(df["earnings_date"])
        log.info("Loaded %d raw transcript rows.", len(df))
        return df
    except Exception as e:
        log.error("Failed to load transcripts: %s", e)
        return pd.DataFrame()


def load_options(path="data/options"):
    try:
        df = pd.read_parquet(path, engine="pyarrow")
        df["ticker"] = df["ticker"].astype(str)
        df["earnings_date"] = pd.to_datetime(df["earnings_date"])
        log.info("Loaded %d raw options rows.", len(df))
        return df
    except Exception as e:
        log.warning("No options data found at %s: %s", path, e)
        return pd.DataFrame()


def align_events(transcripts, price_features, options):
    log.info("Starting event alignment.")
    row_counts = {"transcripts_raw": len(transcripts)}

    if transcripts.empty:
        log.error("Transcript DataFrame is empty — cannot align.")
        return pd.DataFrame(), row_counts

    price_df = pd.DataFrame(price_features)
    row_counts["price_records"] = len(price_df)

    if price_df.empty:
        log.warning(
            "No price feature records — forward returns will be null."
        )
        merged = transcripts.copy()
        for col in [
            "return_1d", "return_3d", "return_5d", "return_10d",
            "realized_vol_1d", "realized_vol_3d",
            "realized_vol_5d", "realized_vol_10d",
        ]:
            merged[col] = None
    else:
        price_df["earnings_date"] = pd.to_datetime(price_df["earnings_date"])
        merged = transcripts.merge(
            price_df, on=["ticker", "earnings_date"], how="left"
        )

    row_counts["after_price_join"] = len(merged)

    if not options.empty:
        options_agg = (
            options.sort_values("expiry")
            .groupby(["ticker", "earnings_date"])
            .agg(
                options_volume=("volume", "sum"),
                avg_iv=("implied_volatility", "mean"),
                avg_pc_ratio=("put_call_ratio", "mean"),
            )
            .reset_index()
        )
        merged = merged.merge(
            options_agg, on=["ticker", "earnings_date"], how="left"
        )
        log.info(
            "Joined options data: %d events matched.",
            merged["avg_iv"].notna().sum(),
        )
    else:
        merged["options_volume"] = None
        merged["avg_iv"] = None
        merged["avg_pc_ratio"] = None

    row_counts["after_options_join"] = len(merged)

    pre_dedup = len(merged)
    merged = merged.drop_duplicates(subset=["ticker", "earnings_date"])
    dropped = pre_dedup - len(merged)
    if dropped:
        log.warning("Dropped %d duplicate earnings events.", dropped)
    row_counts["after_dedup"] = len(merged)

    merged["alignment_flag"] = "ok"
    no_returns = merged["return_1d"].isna()
    merged.loc[no_returns, "alignment_flag"] = "missing_price_data"

    excluded = merged[merged["alignment_flag"] != "ok"]
    if not excluded.empty:
        log.warning(
            "%d events flagged for alignment issues: %s",
            len(excluded),
            excluded["alignment_flag"].value_counts().to_dict(),
        )

    row_counts["final"] = len(merged)
    log.info(
        "Alignment complete. Row counts: %s",
        row_counts,
    )
    return merged, row_counts
