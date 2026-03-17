"""
M11: 셔플 최적화 - 테스트
"""
import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m11_shuffle_optimization.problems import (
    broadcast_join,
    handle_skew_join,
    optimize_shuffle_partitions,
    pre_partitioned_joins,
)


@pytest.fixture
def large_orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    data = [(f"O{i:04d}", ["Electronics", "Clothing", "Books", "Food"][i % 4], float(i * 10))
            for i in range(1, 101)]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def small_categories_df(spark):
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("tax_rate", DoubleType(), True),
    ])
    data = [
        ("Electronics", 0.1),
        ("Clothing", 0.05),
        ("Books", 0.0),
        ("Food", 0.03),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def skewed_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    data = (
        [(f"O{i:04d}", "Electronics", float(i * 100)) for i in range(1, 71)] +
        [(f"O{i:04d}", "Clothing", float(i * 50)) for i in range(71, 81)] +
        [(f"O{i:04d}", "Books", float(i * 30)) for i in range(81, 91)] +
        [(f"O{i:04d}", "Food", float(i * 20)) for i in range(91, 101)]
    )
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_broadcast_join_result_count(self, large_orders_df, small_categories_df):
        result = broadcast_join(large_orders_df, small_categories_df, "category")
        assert result.count() == 100

    def test_broadcast_join_columns(self, large_orders_df, small_categories_df):
        result = broadcast_join(large_orders_df, small_categories_df, "category")
        assert "tax_rate" in result.columns
        assert "order_id" in result.columns

    def test_broadcast_join_data_correct(self, large_orders_df, small_categories_df):
        result = broadcast_join(large_orders_df, small_categories_df, "category")
        electronics = result.filter("category = 'Electronics'").first()
        assert electronics["tax_rate"] == 0.1


class TestMedium:
    def test_optimize_shuffle_partitions_returns_dict(self, spark, large_orders_df):
        def query(df):
            return df.groupBy("category").count()
        result = optimize_shuffle_partitions(spark, large_orders_df, query)
        assert isinstance(result, dict)
        assert "partitions_4" in result
        assert "partitions_200" in result

    def test_optimize_shuffle_partitions_positive_times(self, spark, large_orders_df):
        def query(df):
            return df.groupBy("category").count()
        result = optimize_shuffle_partitions(spark, large_orders_df, query)
        assert result["partitions_4"] >= 0
        assert result["partitions_200"] >= 0

    def test_pre_partitioned_joins_count(self, large_orders_df, small_categories_df):
        result = pre_partitioned_joins(large_orders_df, small_categories_df, "category", 4)
        assert result.count() == 100

    def test_pre_partitioned_joins_columns(self, large_orders_df, small_categories_df):
        result = pre_partitioned_joins(large_orders_df, small_categories_df, "category", 4)
        assert "tax_rate" in result.columns


class TestHard:
    def test_handle_skew_join_count(self, skewed_df, small_categories_df):
        result = handle_skew_join(skewed_df, small_categories_df, "category")
        assert result.count() == 100

    def test_handle_skew_join_no_salt_column(self, skewed_df, small_categories_df):
        result = handle_skew_join(skewed_df, small_categories_df, "category")
        assert "salted_key" not in result.columns
        assert "salt" not in result.columns

    def test_handle_skew_join_tax_rate_present(self, skewed_df, small_categories_df):
        result = handle_skew_join(skewed_df, small_categories_df, "category")
        assert "tax_rate" in result.columns
