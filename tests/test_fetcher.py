import csv
import io
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fetcher import RESERVED_NAMESPACES, WikipediaFetcher

HEADERS = {"User-Agent": "TestBot/0.1"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw_data(articles):
    """Wrap a list of article dicts in the WikiMedia API response envelope."""
    return {"items": [{"articles": articles}]}


def _article(name, views=1000, rank=1):
    return {"article": name, "views": views, "rank": rank}


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_csv_mode_stores_path(self, tmp_path):
        fetcher = WikipediaFetcher(HEADERS, mode="csv", csv_path=str(tmp_path / "pv.csv"))
        assert fetcher.csv_path == tmp_path / "pv.csv"
        assert fetcher.mode == "csv"

    def test_sql_mode_creates_connection(self, tmp_path):
        db = str(tmp_path / "test.db")
        fetcher = WikipediaFetcher(HEADERS, mode="sql", db_path=db)
        assert hasattr(fetcher, "conn")
        assert hasattr(fetcher, "cursor")
        fetcher.close_connection()

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            WikipediaFetcher(HEADERS, mode="parquet")


# ---------------------------------------------------------------------------
# parse_raw_data
# ---------------------------------------------------------------------------

class TestParseRawData:
    @pytest.fixture
    def fetcher(self, tmp_path):
        return WikipediaFetcher(HEADERS, mode="csv", csv_path=str(tmp_path / "pv.csv"))

    def test_none_input_returns_none(self, fetcher):
        assert fetcher.parse_raw_data(None, "2024/01/01") is None

    def test_empty_dict_returns_none(self, fetcher):
        assert fetcher.parse_raw_data({}, "2024/01/01") is None

    def test_normal_articles_preserved(self, fetcher):
        raw = _make_raw_data([
            _article("Albert_Einstein", views=50000, rank=1),
            _article("Python_(programming_language)", views=40000, rank=2),
        ])
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert list(df["article"]) == ["Albert_Einstein", "Python_(programming_language)"]

    def test_main_page_filtered(self, fetcher):
        raw = _make_raw_data([
            _article("Main_Page", views=9999999, rank=1),
            _article("Albert_Einstein", views=50000, rank=2),
        ])
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert "Main_Page" not in df["article"].values

    def test_reserved_namespaces_filtered(self, fetcher):
        namespace_articles = [
            _article(f"{ns}:Some_Title", views=100, rank=i + 1)
            for i, ns in enumerate(RESERVED_NAMESPACES)
        ]
        real_article = _article("Albert_Einstein", views=50000, rank=len(RESERVED_NAMESPACES) + 1)
        raw = _make_raw_data(namespace_articles + [real_article])
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert list(df["article"]) == ["Albert_Einstein"]

    def test_ranks_recalculated_from_one(self, fetcher):
        raw = _make_raw_data([
            _article("Main_Page", views=9999999, rank=1),
            _article("Albert_Einstein", views=50000, rank=2),
            _article("Nikola_Tesla", views=40000, rank=3),
        ])
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert list(df["rank"]) == [1, 2]

    def test_date_assigned_to_all_rows(self, fetcher):
        raw = _make_raw_data([
            _article("Albert_Einstein", views=50000, rank=1),
            _article("Nikola_Tesla", views=40000, rank=2),
        ])
        df = fetcher.parse_raw_data(raw, "2024/06/15")
        assert all(df["date"] == "2024/06/15")

    def test_output_limited_to_100_rows(self, fetcher):
        articles = [_article(f"Article_{i}", views=10000 - i, rank=i + 1) for i in range(150)]
        raw = _make_raw_data(articles)
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert len(df) == 100

    def test_output_columns(self, fetcher):
        raw = _make_raw_data([_article("Albert_Einstein", views=50000, rank=1)])
        df = fetcher.parse_raw_data(raw, "2024/01/01")
        assert set(df.columns) == {"article", "views", "rank", "date"}


# ---------------------------------------------------------------------------
# read_last_csv_row
# ---------------------------------------------------------------------------

class TestReadLastCsvRow:
    def test_reads_last_row(self, tmp_path):
        csv_file = tmp_path / "pv.csv"
        csv_file.write_text(
            "rank,article,views,date\n"
            "1,Albert_Einstein,50000,2024/01/01\n"
            "2,Nikola_Tesla,40000,2024/01/02\n"
        )
        fetcher = WikipediaFetcher(HEADERS, mode="csv", csv_path=str(csv_file))
        row = fetcher.read_last_csv_row()
        assert row == ["2", "Nikola_Tesla", "40000", "2024/01/02"]

    def test_single_data_row(self, tmp_path):
        csv_file = tmp_path / "pv.csv"
        csv_file.write_text("rank,article,views,date\n1,Albert_Einstein,50000,2024/01/01\n")
        fetcher = WikipediaFetcher(HEADERS, mode="csv", csv_path=str(csv_file))
        row = fetcher.read_last_csv_row()
        assert row[-1] == "2024/01/01"


# ---------------------------------------------------------------------------
# insert_data – CSV mode
# ---------------------------------------------------------------------------

class TestInsertDataCsv:
    def test_creates_csv_with_header(self, tmp_path):
        csv_file = tmp_path / "pv.csv"
        fetcher = WikipediaFetcher(HEADERS, mode="csv", csv_path=str(csv_file))
        df = pd.DataFrame([{"rank": 1, "article": "Albert_Einstein", "views": 50000, "date": "2024/01/01"}])
        fetcher.insert_data(df)
        content = csv_file.read_text()
        assert content.startswith("rank,article,views,date")
        assert "Albert_Einstein" in content

    def test_appends_without_duplicate_header(self, tmp_path):
        csv_file = tmp_path / "pv.csv"
        csv_file.write_text("rank,article,views,date\n1,Albert_Einstein,50000,2024/01/01\n")
        fetcher = WikipediaFetcher(HEADERS, mode="csv", csv_path=str(csv_file))
        df = pd.DataFrame([{"rank": 1, "article": "Nikola_Tesla", "views": 40000, "date": "2024/01/02"}])
        fetcher.insert_data(df)
        lines = csv_file.read_text().strip().splitlines()
        header_count = sum(1 for line in lines if line.startswith("rank,"))
        assert header_count == 1
        assert any("Nikola_Tesla" in line for line in lines)


# ---------------------------------------------------------------------------
# fetch_pageviews (HTTP)
# ---------------------------------------------------------------------------

class TestFetchPageviews:
    @pytest.fixture
    def fetcher(self, tmp_path):
        return WikipediaFetcher(HEADERS, mode="csv", csv_path=str(tmp_path / "pv.csv"))

    def test_returns_json_on_success(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"items": []}
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_pageviews("2024/01/01")
        assert result == {"items": []}

    def test_returns_none_on_failure(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = False
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_pageviews("2024/01/01")
        assert result is None

    def test_url_contains_date(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {}
        with patch("fetcher.requests.get", return_value=mock_response) as mock_get:
            fetcher.fetch_pageviews("2024/06/15")
        called_url = mock_get.call_args[0][0]
        assert "2024/06/15" in called_url


# ---------------------------------------------------------------------------
# fetch_article_categories (HTTP)
# ---------------------------------------------------------------------------

class TestFetchArticleCategories:
    @pytest.fixture
    def fetcher(self, tmp_path):
        return WikipediaFetcher(HEADERS, mode="csv", csv_path=str(tmp_path / "pv.csv"))

    def test_returns_json_on_success(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"query": {}}
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_article_categories("Albert_Einstein")
        assert result == {"query": {}}

    def test_returns_none_on_failure(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = False
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_article_categories("Albert_Einstein")
        assert result is None


# ---------------------------------------------------------------------------
# fetch_article_text (HTTP)
# ---------------------------------------------------------------------------

class TestFetchArticleText:
    @pytest.fixture
    def fetcher(self, tmp_path):
        return WikipediaFetcher(HEADERS, mode="csv", csv_path=str(tmp_path / "pv.csv"))

    def test_returns_json_on_success(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"query": {"pages": {}}}
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_article_text("Albert_Einstein")
        assert result == {"query": {"pages": {}}}

    def test_returns_none_on_failure(self, fetcher):
        mock_response = MagicMock()
        mock_response.ok = False
        with patch("fetcher.requests.get", return_value=mock_response):
            result = fetcher.fetch_article_text("Albert_Einstein")
        assert result is None


# ---------------------------------------------------------------------------
# SQL mode
# ---------------------------------------------------------------------------

class TestSqlMode:
    @pytest.fixture
    def fetcher(self, tmp_path):
        f = WikipediaFetcher(HEADERS, mode="sql", db_path=str(tmp_path / "test.db"))
        yield f
        f.close_connection()

    def test_init_db_creates_tables(self, fetcher):
        fetcher.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in fetcher.cursor.fetchall()}
        assert "article" in tables
        assert "pageview" in tables

    def test_get_article_id_creates_new(self, fetcher):
        article_id = fetcher.get_article_id("Albert_Einstein")
        assert isinstance(article_id, int)
        assert article_id > 0

    def test_get_article_id_returns_same_for_existing(self, fetcher):
        id1 = fetcher.get_article_id("Albert_Einstein")
        id2 = fetcher.get_article_id("Albert_Einstein")
        assert id1 == id2

    def test_get_article_id_different_for_different_articles(self, fetcher):
        id1 = fetcher.get_article_id("Albert_Einstein")
        id2 = fetcher.get_article_id("Nikola_Tesla")
        assert id1 != id2

    def test_insert_data_sql_mode(self, fetcher):
        df = pd.DataFrame([
            {"rank": 1, "article": "Albert_Einstein", "views": 50000, "date": "2024/01/01"},
            {"rank": 2, "article": "Nikola_Tesla", "views": 40000, "date": "2024/01/01"},
        ])
        fetcher.insert_data(df)
        fetcher.cursor.execute("SELECT COUNT(*) FROM pageview")
        count = fetcher.cursor.fetchone()[0]
        assert count == 2

    def test_close_connection_removes_conn(self, tmp_path):
        fetcher = WikipediaFetcher(HEADERS, mode="sql", db_path=str(tmp_path / "close_test.db"))
        fetcher.close_connection()
        assert not hasattr(fetcher, "conn")
        assert not hasattr(fetcher, "cursor")
