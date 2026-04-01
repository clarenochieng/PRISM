import os

import pandas as pd
from tqdm import tqdm

from src.collectors.price_collector import collect_price_features
from src.processors.event_aligner import (
    align_events,
    load_options,
    load_transcripts,
)
from src.processors.quality_reporter import generate_report
from src.processors.transcript_cleaner import clean_transcript
from src.utils.logger import get_logger

log = get_logger(__name__)

MASTER_OUTPUT = "data/master_dataset.parquet"
QUALITY_REPORT = "data/quality_report.json"


def run_phase2():
    log.info("=" * 60)
    log.info("PRISMS Phase 2: Data Processing & Alignment")
    log.info("=" * 60)

    row_counts = {}

    log.info("Step 1 — Loading Phase 1 outputs.")
    transcripts = load_transcripts()
    options = load_options()
    row_counts["transcripts_raw"] = len(transcripts)
    row_counts["options_raw"] = len(options)

    if transcripts.empty:
        log.error("No transcript data found. Run Phase 1 first.")
        return

    log.info("Step 2 — Cleaning transcripts.")
    cleaned_rows = []
    flagged_counts = {}

    for _, row in tqdm(
        transcripts.iterrows(),
        total=len(transcripts),
        desc="Cleaning Transcripts",
    ):
        result = clean_transcript(row.get("raw_transcript", ""))
        cleaned = row.to_dict()
        cleaned["cleaned_text"] = result["cleaned_text"]
        cleaned["remarks"] = result["remarks"]
        cleaned["qa"] = result["qa"]
        cleaned["char_count"] = result["char_count"]
        cleaned["quality_flag"] = result["quality_flag"]
        cleaned_rows.append(cleaned)

        flag = result["quality_flag"]
        flagged_counts[flag] = flagged_counts.get(flag, 0) + 1

    transcripts_cleaned = pd.DataFrame(cleaned_rows)
    row_counts["after_cleaning"] = len(transcripts_cleaned)

    log.info(
        "Cleaning complete. Flag breakdown: %s", flagged_counts
    )

    retained = transcripts_cleaned[
        ~transcripts_cleaned["quality_flag"].isin(["missing_text"])
    ]
    dropped = len(transcripts_cleaned) - len(retained)
    if dropped:
        log.warning(
            "Dropped %d records with missing_text flag.", dropped
        )
    row_counts["after_quality_filter"] = len(retained)

    log.info("Step 3 — Collecting historical price data and forward returns.")
    all_price_features = []
    tickers = retained["ticker"].unique()

    for ticker in tqdm(tickers, desc="Fetching Price Data"):
        dates = (
            retained[retained["ticker"] == ticker]["earnings_date"]
            .astype(str)
            .tolist()
        )
        features = collect_price_features(ticker, dates)
        all_price_features.extend(features)
        log.debug(
            "Price features for %s: %d records.", ticker, len(features)
        )

    row_counts["price_feature_records"] = len(all_price_features)
    log.info(
        "Price collection complete: %d feature records across %d tickers.",
        len(all_price_features),
        len(tickers),
    )

    log.info("Step 4 — Aligning events into master dataset.")
    master, align_counts = align_events(retained, all_price_features, options)
    row_counts.update(align_counts)

    log.info("Step 5 — Dropping raw transcript to reduce storage size.")
    if "raw_transcript" in master.columns:
        master = master.drop(columns=["raw_transcript"])

    log.info("Step 6 — Writing master Parquet dataset.")
    os.makedirs(os.path.dirname(MASTER_OUTPUT), exist_ok=True)
    master.to_parquet(MASTER_OUTPUT, engine="pyarrow", index=False)
    log.info(
        "Master dataset written: %s (%d rows, %d cols).",
        MASTER_OUTPUT,
        len(master),
        len(master.columns),
    )

    log.info("Step 7 — Generating data quality report.")
    report = generate_report(master, row_counts, QUALITY_REPORT)

    log.info("=" * 60)
    log.info("Phase 2 complete.")
    log.info(
        "Final rows: %d | Target: %d | Status: %s",
        report["final_row_count"],
        report["target_row_count"],
        "MET" if report["target_met"] else "SHORTFALL",
    )
    log.info("=" * 60)

    return master, report


if __name__ == "__main__":
    run_phase2()
