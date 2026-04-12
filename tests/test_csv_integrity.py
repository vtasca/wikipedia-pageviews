"""
Data integrity checks for pageviews.csv.

These tests give ongoing confidence that the daily collection pipeline is
working correctly.  They are designed to be run against the live file in the
repository, so they intentionally reference the real CSV rather than a fixture.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

CSV_PATH = Path(__file__).parent.parent / "pageviews.csv"
EXPECTED_COLUMNS = ["rank", "article", "views", "date"]
DATE_FORMAT = "%Y/%m/%d"
KNOWN_START_DATE = "2016/01/01"
ROWS_PER_DATE = 100
# WikiMedia API has a 2-day lag; allow a further 3-day buffer for CI scheduling.
MAX_DATA_AGE_DAYS = 5
# Gaps up to 2 days are known to happen (missed CI runs); flag anything larger.
MAX_ALLOWED_GAP_DAYS = 3
# 7 null article values exist in historical data; allow a small margin.
MAX_NULL_ARTICLES = 10


@pytest.fixture(scope="module")
def df():
    return pd.read_csv(CSV_PATH)


@pytest.fixture(scope="module")
def dates(df):
    return sorted(df["date"].unique())


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_expected_columns_present(self, df):
        assert list(df.columns) == EXPECTED_COLUMNS

    def test_rank_is_integer(self, df):
        assert pd.api.types.is_integer_dtype(df["rank"])

    def test_views_is_integer(self, df):
        assert pd.api.types.is_integer_dtype(df["views"])

    def test_no_nulls_in_rank(self, df):
        assert df["rank"].isnull().sum() == 0

    def test_no_nulls_in_views(self, df):
        assert df["views"].isnull().sum() == 0

    def test_no_nulls_in_date(self, df):
        assert df["date"].isnull().sum() == 0

    def test_null_articles_within_acceptable_threshold(self, df):
        null_count = df["article"].isnull().sum()
        assert null_count <= MAX_NULL_ARTICLES, (
            f"{null_count} null article values found (threshold: {MAX_NULL_ARTICLES})"
        )


# ---------------------------------------------------------------------------
# Row / date structure
# ---------------------------------------------------------------------------

class TestStructure:
    def test_total_rows_divisible_by_100(self, df):
        assert len(df) % ROWS_PER_DATE == 0

    def test_every_date_has_exactly_100_rows(self, df):
        counts = df.groupby("date")["rank"].count()
        bad = counts[counts != ROWS_PER_DATE]
        assert bad.empty, f"Dates with wrong row count:\n{bad}"

    def test_ranks_are_1_to_100_per_date(self, df):
        def check(group):
            return sorted(group["rank"].tolist()) == list(range(1, ROWS_PER_DATE + 1))
        bad_dates = [date for date, group in df.groupby("date") if not check(group)]
        assert not bad_dates, f"Dates with incorrect rank sequence: {bad_dates[:5]}"

    def test_no_duplicate_rank_per_date(self, df):
        dups = df.duplicated(subset=["date", "rank"]).sum()
        assert dups == 0, f"{dups} duplicate (date, rank) pairs found"

    def test_no_duplicate_article_per_date(self, df):
        dups = df.dropna(subset=["article"]).duplicated(subset=["date", "article"]).sum()
        assert dups == 0, f"{dups} duplicate (date, article) pairs found"


# ---------------------------------------------------------------------------
# Value sanity
# ---------------------------------------------------------------------------

class TestValues:
    def test_views_are_positive(self, df):
        assert (df["views"] > 0).all(), "Some views values are zero or negative"

    def test_all_dates_match_expected_format(self, df):
        invalid = df["date"][
            ~df["date"].str.match(r"^\d{4}/\d{2}/\d{2}$")
        ]
        assert invalid.empty, f"Dates with unexpected format: {invalid.unique()[:5]}"

    def test_no_main_page_articles(self, df):
        assert (df["article"] != "Main_Page").all()

    def test_no_reserved_namespace_articles(self, df):
        from fetcher import RESERVED_NAMESPACES
        pattern = "|".join(f"{ns}:" for ns in RESERVED_NAMESPACES)
        mask = df["article"].dropna().str.contains(pattern)
        assert not mask.any(), "Reserved namespace articles found in CSV"


# ---------------------------------------------------------------------------
# Date continuity and freshness
# ---------------------------------------------------------------------------

class TestDates:
    def test_data_starts_at_known_start_date(self, dates):
        assert dates[0] == KNOWN_START_DATE

    def test_most_recent_date_is_fresh(self, dates):
        latest = datetime.strptime(dates[-1], DATE_FORMAT)
        age = (datetime.now() - latest).days
        assert age <= MAX_DATA_AGE_DAYS, (
            f"Most recent data is {age} days old (latest: {dates[-1]}). "
            f"The pipeline may not be running."
        )

    def test_no_large_gaps_between_dates(self, dates):
        large_gaps = []
        for i in range(1, len(dates)):
            d1 = datetime.strptime(dates[i - 1], DATE_FORMAT)
            d2 = datetime.strptime(dates[i], DATE_FORMAT)
            delta = (d2 - d1).days
            if delta > MAX_ALLOWED_GAP_DAYS:
                large_gaps.append((dates[i - 1], dates[i], delta))
        assert not large_gaps, (
            f"Gaps larger than {MAX_ALLOWED_GAP_DAYS} days found: {large_gaps}"
        )
