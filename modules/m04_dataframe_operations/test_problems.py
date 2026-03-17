"""
M04: DataFrame 연산 - 테스트
"""
import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from modules.m04_dataframe_operations.problems import (
    add_total_column,
    calculate_delivery_days,
    categorize_orders,
    clean_address_column,
    filter_and_sort,
)


@pytest.fixture
def orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("delivery_date", StringType(), True),
    ])
    data = [
        ("O001", 1200.0, 1, "Electronics", "2023-01-05", "2023-01-10"),
        ("O002", 85.5, 2, "Clothing", "2023-01-06", "2023-01-11"),
        ("O003", 29.99, 3, "Books", "2023-01-07", "2023-01-12"),
        ("O004", 450.0, 1, "Electronics", "2023-01-08", "2023-01-15"),
        ("O005", 35.20, 5, "Food", "2023-01-09", "2023-01-10"),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def address_df(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("address_raw", StringType(), True),
    ])
    data = [
        ("1", "123 Main Street Seoul 06100"),
        ("2", "서울시 강남구 테헤란로 06035"),
        ("3", "no zip code here"),
        ("4", "456 Broadway New York 10013"),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_add_total_column_exists(self, orders_df):
        result = add_total_column(orders_df)
        assert "total" in result.columns

    def test_add_total_column_values(self, orders_df):
        result = add_total_column(orders_df)
        row = result.filter("order_id = 'O001'").first()
        assert abs(row["total"] - 1200.0) < 0.01  # 1200.0 * 1

    def test_add_total_column_calculation(self, orders_df):
        result = add_total_column(orders_df)
        row = result.filter("order_id = 'O002'").first()
        assert abs(row["total"] - 171.0) < 0.01  # 85.5 * 2

    def test_filter_and_sort_min_amount(self, orders_df):
        result = filter_and_sort(orders_df, 100.0, "amount")
        rows = result.collect()
        for row in rows:
            assert row["amount"] >= 100.0

    def test_filter_and_sort_order(self, orders_df):
        result = filter_and_sort(orders_df, 0.0, "amount")
        amounts = [row["amount"] for row in result.collect()]
        assert amounts == sorted(amounts)

    def test_filter_and_sort_count(self, orders_df):
        result = filter_and_sort(orders_df, 100.0, "amount")
        # O001(1200), O004(450) >= 100
        assert result.count() == 2


class TestMedium:
    def test_categorize_orders_column_exists(self, orders_df):
        result = categorize_orders(orders_df)
        assert "order_tier" in result.columns

    def test_categorize_orders_premium(self, orders_df):
        result = categorize_orders(orders_df)
        row = result.filter("order_id = 'O001'").first()
        assert row["order_tier"] == "Premium"

    def test_categorize_orders_standard(self, orders_df):
        result = categorize_orders(orders_df)
        row = result.filter("order_id = 'O002'").first()
        assert row["order_tier"] == "Standard"

    def test_categorize_orders_basic(self, orders_df):
        result = categorize_orders(orders_df)
        row = result.filter("order_id = 'O003'").first()
        assert row["order_tier"] == "Basic"

    def test_calculate_delivery_days_column_exists(self, orders_df):
        result = calculate_delivery_days(orders_df)
        assert "delivery_days" in result.columns

    def test_calculate_delivery_days_values(self, orders_df):
        result = calculate_delivery_days(orders_df)
        row = result.filter("order_id = 'O001'").first()
        assert row["delivery_days"] == 5  # Jan 5 to Jan 10

    def test_calculate_delivery_days_positive(self, orders_df):
        result = calculate_delivery_days(orders_df)
        for row in result.collect():
            assert row["delivery_days"] >= 0


class TestHard:
    def test_clean_address_column_exists(self, address_df):
        result = clean_address_column(address_df)
        assert "zip_code" in result.columns

    def test_clean_address_extracts_zip(self, address_df):
        result = clean_address_column(address_df)
        row = result.filter("id = '1'").first()
        assert row["zip_code"] == "06100"

    def test_clean_address_korean(self, address_df):
        result = clean_address_column(address_df)
        row = result.filter("id = '2'").first()
        assert row["zip_code"] == "06035"

    def test_clean_address_no_zip(self, address_df):
        result = clean_address_column(address_df)
        row = result.filter("id = '3'").first()
        assert row["zip_code"] == ""  # No match returns empty string
