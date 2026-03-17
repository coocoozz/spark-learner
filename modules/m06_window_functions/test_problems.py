"""
M06: 윈도우 함수 - 테스트
"""
import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from modules.m06_window_functions.problems import (
    cumulative_volume,
    daily_returns_and_streaks,
    max_drawdown,
    moving_averages,
    rank_by_price,
)


@pytest.fixture
def stock_df(spark):
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ])
    data = [
        ("AAPL", "2023-01-02", 125.07, 112117500),
        ("AAPL", "2023-01-03", 125.35, 89113600),
        ("AAPL", "2023-01-04", 130.15, 70790800),
        ("AAPL", "2023-01-05", 125.02, 80962700),
        ("AAPL", "2023-01-06", 129.62, 87754700),
        ("GOOGL", "2023-01-02", 88.73, 23814000),
        ("GOOGL", "2023-01-03", 86.69, 21673000),
        ("GOOGL", "2023-01-04", 89.44, 24553000),
        ("GOOGL", "2023-01-05", 87.09, 23428000),
        ("GOOGL", "2023-01-06", 87.55, 24851000),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_rank_by_price_column_exists(self, stock_df):
        result = rank_by_price(stock_df)
        assert "price_rank" in result.columns

    def test_rank_by_price_rank1_is_highest(self, stock_df):
        result = rank_by_price(stock_df)
        date_row = result.filter("date = '2023-01-04'").orderBy("price_rank").first()
        assert date_row["symbol"] == "AAPL"
        assert date_row["price_rank"] == 1

    def test_cumulative_volume_column_exists(self, stock_df):
        result = cumulative_volume(stock_df)
        assert "cumulative_volume" in result.columns

    def test_cumulative_volume_increasing(self, stock_df):
        result = cumulative_volume(stock_df)
        aapl = result.filter("symbol = 'AAPL'").orderBy("date").collect()
        volumes = [row["cumulative_volume"] for row in aapl]
        assert volumes == sorted(volumes)

    def test_cumulative_volume_first_equals_volume(self, stock_df):
        result = cumulative_volume(stock_df)
        first = result.filter("symbol = 'AAPL'").orderBy("date").first()
        assert first["cumulative_volume"] == first["volume"]


class TestMedium:
    def test_moving_averages_columns_exist(self, stock_df):
        result = moving_averages(stock_df, [3, 5])
        assert "ma_3" in result.columns
        assert "ma_5" in result.columns

    def test_moving_averages_ma3_third_row(self, stock_df):
        result = moving_averages(stock_df, [3])
        aapl = result.filter("symbol = 'AAPL'").orderBy("date").collect()
        expected = (125.07 + 125.35 + 130.15) / 3
        assert abs(aapl[2]["ma_3"] - expected) < 0.01

    def test_daily_returns_columns_exist(self, stock_df):
        result = daily_returns_and_streaks(stock_df)
        assert "daily_return" in result.columns
        assert "is_up" in result.columns

    def test_daily_returns_first_row_null(self, stock_df):
        result = daily_returns_and_streaks(stock_df)
        first_aapl = result.filter("symbol = 'AAPL'").orderBy("date").first()
        assert first_aapl["prev_close"] is None

    def test_daily_returns_second_row(self, stock_df):
        result = daily_returns_and_streaks(stock_df)
        aapl = result.filter("symbol = 'AAPL'").orderBy("date").collect()
        expected = (125.35 - 125.07) / 125.07 * 100
        assert abs(aapl[1]["daily_return"] - expected) < 0.01


class TestHard:
    def test_max_drawdown_columns(self, stock_df):
        result = max_drawdown(stock_df)
        assert "running_max" in result.columns
        assert "drawdown" in result.columns

    def test_max_drawdown_non_negative(self, stock_df):
        result = max_drawdown(stock_df)
        for row in result.collect():
            assert row["drawdown"] >= 0

    def test_max_drawdown_sorted(self, stock_df):
        result = max_drawdown(stock_df)
        drawdowns = [row["drawdown"] for row in result.collect()]
        assert drawdowns == sorted(drawdowns, reverse=True)

    def test_running_max_non_decreasing_per_symbol(self, stock_df):
        result = max_drawdown(stock_df)
        for symbol in ["AAPL", "GOOGL"]:
            rows = result.filter(f"symbol = '{symbol}'").orderBy("date").collect()
            maxes = [r["running_max"] for r in rows]
            for i in range(1, len(maxes)):
                assert maxes[i] >= maxes[i - 1]
