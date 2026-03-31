import glob
import os

import pandas as pd

from src.utils.logger import get_logger

log = get_logger(__name__)


class DataStorage:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.transcripts_path = os.path.join(base_path, "transcripts")
        self.options_path = os.path.join(base_path, "options")

        os.makedirs(self.transcripts_path, exist_ok=True)
        os.makedirs(self.options_path, exist_ok=True)

    def _clear_partitions(self, base_path, df, partition_cols):
        for _, row in df[partition_cols].drop_duplicates().iterrows():
            partition_path = base_path
            for col in partition_cols:
                partition_path = os.path.join(
                    partition_path, f"{col}={row[col]}"
                )
            removed = 0
            for f in glob.glob(os.path.join(partition_path, "*.parquet")):
                os.remove(f)
                removed += 1
            if removed:
                log.debug(
                    "Cleared %d existing file(s) from partition %s.",
                    removed,
                    partition_path,
                )

    def save_transcripts(self, df):
        if df.empty:
            log.warning(
                "save_transcripts called with empty DataFrame — skipping."
            )
            return

        df["year"] = df["year"].astype(str)
        self._clear_partitions(self.transcripts_path, df, ["year", "ticker"])

        df.to_parquet(
            self.transcripts_path,
            engine="pyarrow",
            partition_cols=["year", "ticker"],
            index=False,
        )
        log.info(
            "Saved %d transcript records to %s.",
            len(df),
            self.transcripts_path,
        )

    def save_options(self, df):
        if df.empty:
            log.warning("save_options called with empty DataFrame — skipping.")
            return

        df["year"] = df["year"].astype(str)
        self._clear_partitions(self.options_path, df, ["year", "ticker"])

        df.to_parquet(
            self.options_path,
            engine="pyarrow",
            partition_cols=["year", "ticker"],
            index=False,
        )
        log.info("Saved %d options records to %s.", len(df), self.options_path)


if __name__ == "__main__":
    storage = DataStorage()
    test_df = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "earnings_date": "2026-01-20",
                "fiscal_quarter": "Q1",
                "fiscal_year": 2026,
                "year": "2026",
                "raw_transcript": "Sample transcript text...",
            }
        ]
    )
    storage.save_transcripts(test_df)

    log.info("Directory structure after save:")
    for root, dirs, files in os.walk("data/transcripts"):
        log.info("%s: %s", root, files)
