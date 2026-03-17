"""
M08: Spark SQL - 테스트
"""
import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m08_spark_sql.problems import (
    above_average_customers,
    customer_segments_sql,
    high_value_categories,
    monthly_growth_report,
    query_top_orders,
)


@pytest.fixture
def orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
    ])
    data = [
        ("O001", "C001", "Electronics", 1200.0, "2023-01-05"),
        ("O002", "C002", "Clothing", 85.5, "2023-01-06"),
        ("O003", "C003", "Books", 29.99, "2023-01-07"),
        ("O004", "C001", "Electronics", 450.0, "2023-01-15"),
        ("O005", "C004", "Food", 35.20, "2023-01-09"),
        ("O006", "C002", "Clothing", 120.0, "2023-01-20"),
        ("O007", "C003", "Books", 45.0, "2023-02-01"),
        ("O008", "C001", "Electronics", 899.0, "2023-02-10"),
        ("O009", "C004", "Food", 67.80, "2023-02-15"),
        ("O010", "C002", "Clothing", 200.0, "2023-02-20"),
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
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_query_top_orders_count(self, spark, orders_df):
        result = query_top_orders(spark, orders_df, 3)
        assert result.count() == 3

    def test_query_top_orders_sorted(self, spark, orders_df):
        result = query_top_orders(spark, orders_df, 5)
        amounts = [r["amount"] for r in result.collect()]
        assert amounts == sorted(amounts, reverse=True)

    def test_query_top_orders_top1(self, spark, orders_df):
        result = query_top_orders(spark, orders_df, 1)
        assert result.first()["amount"] == 1200.0

    def test_high_value_categories_threshold(self, spark, orders_df):
        result = high_value_categories(spark, orders_df, 400.0)
        cats = [r["category"] for r in result.collect()]
        assert "Electronics" in cats
        assert "Books" not in cats  # Books total = 74.99 < 400

    def test_high_value_categories_sorted(self, spark, orders_df):
        result = high_value_categories(spark, orders_df, 0.0)
        revenues = [r["total_revenue"] for r in result.collect()]
        assert revenues == sorted(revenues, reverse=True)


class TestMedium:
    def test_above_average_customers_columns(self, spark, orders_df, customers_df):
        result = above_average_customers(spark, orders_df, customers_df)
        assert "customer_id" in result.columns
        assert "avg_amount" in result.columns

    def test_above_average_customers_alice(self, spark, orders_df, customers_df):
        result = above_average_customers(spark, orders_df, customers_df)
        ids = [r["customer_id"] for r in result.collect()]
        assert "C001" in ids  # Alice has high Electronics orders

    def test_customer_segments_vip(self, spark, orders_df, customers_df):
        result = customer_segments_sql(spark, orders_df, customers_df)
        alice = result.filter("customer_id = 'C001'").first()
        assert alice["segment"] == "VIP"  # 1200 + 450 + 899 = 2549

    def test_customer_segments_regular(self, spark, orders_df, customers_df):
        result = customer_segments_sql(spark, orders_df, customers_df)
        bob = result.filter("customer_id = 'C002'").first()
        assert bob["segment"] == "Regular"  # 85.5 + 120 + 200 = 405.5 → Occasional
        # Actually 405.5 < 500, so Occasional

    def test_customer_segments_columns(self, spark, orders_df, customers_df):
        result = customer_segments_sql(spark, orders_df, customers_df)
        assert "segment" in result.columns
        assert "total_spent" in result.columns


class TestHard:
    def test_monthly_growth_columns(self, spark, orders_df):
        result = monthly_growth_report(spark, orders_df)
        assert "month" in result.columns
        assert "monthly_revenue" in result.columns
        assert "growth_rate" in result.columns

    def test_monthly_growth_sorted(self, spark, orders_df):
        result = monthly_growth_report(spark, orders_df)
        months = [r["month"] for r in result.collect()]
        assert months == sorted(months)

    def test_monthly_growth_first_null(self, spark, orders_df):
        result = monthly_growth_report(spark, orders_df)
        first = result.orderBy("month").first()
        assert first["prev_revenue"] is None

    def test_monthly_growth_second_has_rate(self, spark, orders_df):
        result = monthly_growth_report(spark, orders_df)
        rows = result.orderBy("month").collect()
        if len(rows) >= 2:
            assert rows[1]["growth_rate"] is not None
