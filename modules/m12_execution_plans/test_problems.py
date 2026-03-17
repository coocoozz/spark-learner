"""
M12: 실행 계획 분석 - 테스트
"""
import os
import tempfile

import pytest
from pyspark.sql.functions import broadcast, col
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m12_execution_plans.problems import (
    compare_pushdown,
    identify_join_strategy,
    identify_plan_nodes,
    optimize_query,
)


@pytest.fixture
def orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("category", StringType(), True),
    ])
    data = [
        ("O001", "C001", 1200.0, "Electronics"),
        ("O002", "C002", 85.5, "Clothing"),
        ("O003", "C001", 450.0, "Electronics"),
        ("O004", "C003", 29.99, "Books"),
        ("O005", "C002", 35.20, "Food"),
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
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def categories_df(spark):
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
    ])
    data = [
        ("Electronics", "전자기기"),
        ("Clothing", "의류"),
        ("Books", "도서"),
        ("Food", "식품"),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def parquet_path(spark, orders_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        orders_df.write.partitionBy("category").mode("overwrite").parquet(tmpdir)
        yield tmpdir


class TestEasy:
    def test_identify_plan_nodes_returns_dict(self, orders_df):
        result = identify_plan_nodes(orders_df.filter("amount > 100"))
        assert isinstance(result, dict)

    def test_identify_plan_nodes_has_filter(self, orders_df):
        filtered_df = orders_df.filter(col("amount") > 100)
        result = identify_plan_nodes(filtered_df)
        assert result["has_filter"] is True

    def test_identify_plan_nodes_has_plan_text(self, orders_df):
        result = identify_plan_nodes(orders_df)
        assert isinstance(result["plan_text"], str)
        assert len(result["plan_text"]) > 0

    def test_identify_plan_nodes_has_exchange_after_groupby(self, orders_df):
        grouped = orders_df.groupBy("category").count()
        result = identify_plan_nodes(grouped)
        assert result["has_exchange"] is True


class TestMedium:
    def test_compare_pushdown_returns_dict(self, spark, parquet_path):
        result = compare_pushdown(spark, parquet_path, "category", "Electronics")
        assert isinstance(result, dict)
        assert "pushdown_plan" in result
        assert "has_pushed_filters" in result

    def test_compare_pushdown_plan_not_empty(self, spark, parquet_path):
        result = compare_pushdown(spark, parquet_path, "category", "Electronics")
        assert len(result["pushdown_plan"]) > 0

    def test_identify_join_strategy_broadcast(self, orders_df, customers_df):
        joined = orders_df.join(broadcast(customers_df), on="customer_id")
        strategy = identify_join_strategy(joined)
        assert strategy == "BroadcastHashJoin"

    def test_identify_join_strategy_sort_merge(self, spark, orders_df, customers_df):
        # broadcast threshold를 0으로 설정하여 SortMergeJoin 강제
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        joined = orders_df.join(customers_df, on="customer_id")
        strategy = identify_join_strategy(joined)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
        assert strategy in ["SortMergeJoin", "ShuffledHashJoin"]


class TestHard:
    def test_optimize_query_result_count(self, spark, orders_df, customers_df, categories_df):
        result = optimize_query(spark, orders_df, customers_df, categories_df)
        assert result.count() == 5

    def test_optimize_query_columns(self, spark, orders_df, customers_df, categories_df):
        result = optimize_query(spark, orders_df, customers_df, categories_df)
        assert "order_id" in result.columns
        assert "name" in result.columns
        assert "description" in result.columns

    def test_optimize_query_uses_broadcast(self, spark, orders_df, customers_df, categories_df):
        result = optimize_query(spark, orders_df, customers_df, categories_df)
        plan = result._jdf.queryExecution().simpleString()
        assert "BroadcastHashJoin" in plan
