"""
M05: 조인과 집계 - 테스트
"""
import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m05_joins_and_aggregations.problems import (
    category_metrics,
    cross_summary,
    customers_without_orders,
    order_intervals,
    revenue_by_customer,
)


@pytest.fixture
def orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("order_date", StringType(), True),
    ])
    data = [
        ("O001", "C001", "P010", 1200.0, "Electronics", "2023-01-05"),
        ("O002", "C002", "P020", 85.5, "Clothing", "2023-01-06"),
        ("O003", "C003", "P030", 29.99, "Books", "2023-01-07"),
        ("O004", "C001", "P040", 450.0, "Electronics", "2023-01-15"),
        ("O005", "C004", "P050", 35.20, "Food", "2023-01-09"),
        ("O006", "C002", "P060", 120.0, "Clothing", "2023-01-20"),
        ("O007", "C003", "P070", 45.0, "Books", "2023-02-01"),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def customers_df(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
    ])
    data = [
        ("C001", "Alice", "Seoul"),
        ("C002", "Bob", "Busan"),
        ("C003", "Carol", "Incheon"),
        ("C004", "Dave", "Daegu"),
        ("C005", "Eve", "Seoul"),      # 주문 없음
        ("C006", "Frank", "Daejeon"),  # 주문 없음
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def products_df(spark):
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    data = [
        ("P010", "Laptop", "Electronics", 1200.0),
        ("P020", "T-Shirt", "Clothing", 42.75),
        ("P030", "Book A", "Books", 29.99),
        ("P040", "Headphones", "Electronics", 225.0),
        ("P050", "Granola", "Food", 8.80),
        ("P060", "Jacket", "Clothing", 120.0),
        ("P070", "Book B", "Books", 45.0),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_revenue_by_customer_columns(self, orders_df, customers_df):
        result = revenue_by_customer(orders_df, customers_df)
        assert "customer_id" in result.columns
        assert "name" in result.columns
        assert "total_revenue" in result.columns
        assert "order_count" in result.columns

    def test_revenue_by_customer_c001(self, orders_df, customers_df):
        result = revenue_by_customer(orders_df, customers_df)
        row = result.filter("customer_id = 'C001'").first()
        assert abs(row["total_revenue"] - 1650.0) < 0.01
        assert row["order_count"] == 2

    def test_revenue_by_customer_sorted(self, orders_df, customers_df):
        result = revenue_by_customer(orders_df, customers_df)
        revenues = [r["total_revenue"] for r in result.collect()]
        assert revenues == sorted(revenues, reverse=True)

    def test_customers_without_orders(self, customers_df, orders_df):
        result = customers_without_orders(customers_df, orders_df)
        ids = [r["customer_id"] for r in result.collect()]
        assert "C005" in ids
        assert "C006" in ids
        assert "C001" not in ids

    def test_customers_without_orders_count(self, customers_df, orders_df):
        result = customers_without_orders(customers_df, orders_df)
        assert result.count() == 2


class TestMedium:
    def test_cross_summary_columns(self, orders_df, customers_df, products_df):
        result = cross_summary(orders_df, customers_df, products_df)
        assert "category" in result.columns
        assert "city" in result.columns
        assert "total_revenue" in result.columns

    def test_cross_summary_sorted(self, orders_df, customers_df, products_df):
        result = cross_summary(orders_df, customers_df, products_df)
        revenues = [r["total_revenue"] for r in result.collect()]
        assert revenues == sorted(revenues, reverse=True)

    def test_category_metrics_columns(self, orders_df):
        result = category_metrics(orders_df)
        for col in ["category", "total_revenue", "avg_amount", "order_count", "unique_customers"]:
            assert col in result.columns

    def test_category_metrics_electronics(self, orders_df):
        result = category_metrics(orders_df)
        row = result.filter("category = 'Electronics'").first()
        assert abs(row["total_revenue"] - 1650.0) < 0.01
        assert row["order_count"] == 2
        assert row["unique_customers"] == 1


class TestHard:
    def test_order_intervals_columns(self, orders_df):
        result = order_intervals(orders_df)
        assert "days_since_last_order" in result.columns
        assert "prev_order_date" in result.columns

    def test_order_intervals_no_null_prev(self, orders_df):
        result = order_intervals(orders_df)
        for row in result.collect():
            assert row["prev_order_date"] is not None

    def test_order_intervals_c001(self, orders_df):
        result = order_intervals(orders_df)
        c001 = result.filter("customer_id = 'C001'").first()
        # O001: 2023-01-05, O004: 2023-01-15 → 10일 차이
        assert c001["days_since_last_order"] == 10

    def test_order_intervals_positive(self, orders_df):
        result = order_intervals(orders_df)
        for row in result.collect():
            assert row["days_since_last_order"] >= 0
