"""
M09: 파티셔닝 - 테스트
"""
import os
import tempfile

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m09_partitioning.problems import (
    adjust_partitions,
    analyze_partition_skew,
    salt_skewed_key,
    save_partitioned,
)


@pytest.fixture
def orders_df(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("region", StringType(), True),
    ])
    data = [
        ("O001", "Electronics", 1200.0, "Seoul"),
        ("O002", "Electronics", 450.0, "Seoul"),
        ("O003", "Electronics", 899.0, "Busan"),
        ("O004", "Clothing", 85.5, "Seoul"),
        ("O005", "Clothing", 120.0, "Incheon"),
        ("O006", "Books", 29.99, "Seoul"),
        ("O007", "Food", 35.20, "Daegu"),
        ("O008", "Electronics", 350.0, "Seoul"),
        ("O009", "Electronics", 780.0, "Busan"),
        ("O010", "Electronics", 220.0, "Seoul"),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_adjust_partitions_repartition(self, orders_df):
        result = adjust_partitions(orders_df, 2, use_coalesce=False)
        assert result.rdd.getNumPartitions() == 2

    def test_adjust_partitions_coalesce(self, orders_df):
        # 먼저 파티션을 늘린 후 coalesce로 줄이기
        df_many = orders_df.repartition(8)
        result = adjust_partitions(df_many, 2, use_coalesce=True)
        assert result.rdd.getNumPartitions() == 2

    def test_adjust_partitions_data_preserved(self, orders_df):
        result = adjust_partitions(orders_df, 3)
        assert result.count() == orders_df.count()

    def test_save_partitioned(self, spark, orders_df):
        with tempfile.TemporaryDirectory() as tmpdir:
            save_partitioned(orders_df, tmpdir, "category")
            # 파티션된 디렉토리가 생성되었는지 확인
            loaded = spark.read.parquet(tmpdir)
            assert loaded.count() == orders_df.count()
            # category 컬럼이 있어야 함
            assert "category" in loaded.columns


class TestMedium:
    def test_analyze_partition_skew_columns(self, orders_df):
        result = analyze_partition_skew(orders_df, "category")
        assert "category" in result.columns
        assert "count" in result.columns
        assert "percentage" in result.columns

    def test_analyze_partition_skew_sorted(self, orders_df):
        result = analyze_partition_skew(orders_df, "category")
        counts = [r["count"] for r in result.collect()]
        assert counts == sorted(counts, reverse=True)

    def test_analyze_partition_skew_electronics_top(self, orders_df):
        result = analyze_partition_skew(orders_df, "category")
        top = result.first()
        assert top["category"] == "Electronics"
        assert top["count"] == 6

    def test_analyze_partition_skew_percentage_sum(self, orders_df):
        result = analyze_partition_skew(orders_df, "category")
        total_pct = sum(r["percentage"] for r in result.collect())
        assert abs(total_pct - 100.0) < 0.01


class TestHard:
    def test_salt_skewed_key_column_exists(self, orders_df):
        result = salt_skewed_key(orders_df, "category", 4)
        assert "salted_key" in result.columns

    def test_salt_skewed_key_format(self, orders_df):
        result = salt_skewed_key(orders_df, "category", 4)
        rows = result.collect()
        for row in rows:
            key = row["salted_key"]
            assert "_" in key
            prefix, salt = key.rsplit("_", 1)
            assert prefix == row["category"]
            assert 0 <= int(salt) < 4

    def test_salt_skewed_key_distributes(self, orders_df):
        # Electronics가 6개이므로 4개 salt로 분산되면 여러 salted_key 생성
        electronics = orders_df.filter("category = 'Electronics'")
        result = salt_skewed_key(electronics, "category", 4)
        unique_keys = result.select("salted_key").distinct().count()
        # 6개 행을 4개 salt로 → 최소 1개, 최대 4개 고유 키
        assert 1 <= unique_keys <= 4
