import re

import pandas as pd
import streamlit as st

TRANSCRIPTS_PATH = "data/transcripts"
EARNINGS_RE = re.compile(
    r"item\s+2\.02"
    r"|results of operations"
    r"|quarterly.*results"
    r"|revenue.*quarter"
    r"|earnings per share",
    re.IGNORECASE,
)

st.set_page_config(
    page_title="PRISM — SEC Filing Explorer",
    page_icon="📈",
    layout="wide",
)


@st.cache_data
def load_data():
    df = pd.read_parquet(TRANSCRIPTS_PATH, engine="pyarrow")
    df["earnings_date"] = pd.to_datetime(df["earnings_date"])
    df["year"] = df["earnings_date"].dt.year
    df["char_count"] = df["raw_transcript"].str.len()
    df["is_earnings"] = df["raw_transcript"].str.contains(
        EARNINGS_RE, na=False
    )
    return df


df = load_data()

st.title("📈 PRISM — SEC Filing Explorer")
st.caption(
    "Browse raw 8-K filings collected from SEC EDGAR. "
    "Earnings filings are detected via Item 2.02 / keyword matching."
)

st.divider()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Filings", f"{len(df):,}")
col2.metric("Tickers", f"{df['ticker'].nunique()}")
col3.metric(
    "Earnings Filings",
    f"{df['is_earnings'].sum():,}",
    f"{df['is_earnings'].mean()*100:.1f}%",
)
col4.metric(
    "Date Range",
    f"{df['earnings_date'].min().year}–{df['earnings_date'].max().year}",
)

st.divider()

left, right = st.columns([1, 2])

with left:
    st.subheader("Filters")

    tickers = sorted(df["ticker"].unique().tolist())
    selected_ticker = st.selectbox("Ticker", ["— All —"] + tickers)

    years = sorted(df["year"].unique().tolist(), reverse=True)
    selected_year = st.selectbox("Year", ["— All —"] + years)

    earnings_only = st.checkbox("Earnings filings only", value=False)

    st.subheader("Filings per Year")
    year_counts = (
        df.groupby("year")
        .agg(filings=("ticker", "count"), tickers=("ticker", "nunique"))
        .reset_index()
        .rename(columns={"year": "Year", "filings": "Filings",
                         "tickers": "Tickers"})
    )
    st.dataframe(year_counts, hide_index=True, use_container_width=True)

with right:
    filtered = df.copy()
    if selected_ticker != "— All —":
        filtered = filtered[filtered["ticker"] == selected_ticker]
    if selected_year != "— All —":
        filtered = filtered[filtered["year"] == int(selected_year)]
    if earnings_only:
        filtered = filtered[filtered["is_earnings"]]

    filtered = filtered.sort_values("earnings_date", ascending=False)

    st.subheader(f"Filings ({len(filtered):,} results)")

    display = filtered[
        ["ticker", "company_name", "earnings_date", "char_count",
         "is_earnings"]
    ].copy()
    display["earnings_date"] = display["earnings_date"].dt.date
    display = display.rename(columns={
        "ticker": "Ticker",
        "company_name": "Company",
        "earnings_date": "Filing Date",
        "char_count": "Chars",
        "is_earnings": "Earnings?",
    })

    event = st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        height=320,
    )

    selected_rows = event.selection.rows
    if selected_rows:
        idx = filtered.iloc[selected_rows[0]].name
        row = df.loc[idx]

        st.divider()
        tag = "🟢 Earnings" if row["is_earnings"] else "🔵 Other"
        st.markdown(
            f"**{row['ticker']} — {row['company_name']}** &nbsp; {tag}"
        )
        st.caption(
            f"Filing date: {row['earnings_date'].date()} &nbsp;|&nbsp; "
            f"{row['char_count']:,} chars"
        )

        tab1, tab2 = st.tabs(["📄 Cleaned View", "🔩 Raw Text"])

        with tab1:
            text = row["raw_transcript"]
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"[ \t]{2,}", " ", text)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            st.text_area(
                "Cleaned transcript",
                value=text[:8000],
                height=420,
                label_visibility="collapsed",
            )

        with tab2:
            st.text_area(
                "Raw transcript",
                value=row["raw_transcript"][:8000],
                height=420,
                label_visibility="collapsed",
            )
    else:
        st.info("Click a row above to view the filing text.")
