"""
M10: 캐싱과 영속성 - 테스트
"""
import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from modules.m10_caching_and_persistence.problems import (
    cache_and_verify,
    iterative_with_checkpoint,
    measure_cache_benefit,
    persist_with_level,
)


@pytest.fixture
def transactions_df(spark):
    schema = StructType([
        StructField("tx_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("tx_date", StringType(), True),
    ])
    data = [
        ("T001", "A001", 1500.0, "salary", "2023-01-15"),
        ("T002", "A001", -50.0, "food", "2023-01-16"),
        ("T003", "A002", 3000.0, "salary", "2023-01-31"),
        ("T004", "A002", -120.0, "shopping", "2023-02-05"),
        ("T005", "A003", 800.0, "deposit", "2023-02-10"),
        ("T006", "A001", -200.0, "utilities", "2023-02-15"),
        ("T007", "A003", -75.0, "food", "2023-02-20"),
        ("T008", "A002", -90.0, "transport", "2023-03-01"),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_cache_and_verify_returns_tuple(self, transactions_df):
        result = cache_and_verify(transactions_df)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_cache_and_verify_cached_before(self, transactions_df):
        _, is_cached_before, _ = cache_and_verify(transactions_df)
        assert is_cached_before is True

    def test_cache_and_verify_uncached_after(self, transactions_df):
        _, _, is_cached_after = cache_and_verify(transactions_df)
        assert is_cached_after is False

    def test_cache_and_verify_data_preserved(self, transactions_df):
        cached_df, _, _ = cache_and_verify(transactions_df)
        # 데이터가 그대로 있어야 함 (unpersist 후에도 재계산 가능)
        assert cached_df.count() == transactions_df.count()

    def test_persist_with_level_memory_only(self, transactions_df):
        result = persist_with_level(transactions_df, "MEMORY_ONLY")
        assert result.is_cached
        result.unpersist()

    def test_persist_with_level_memory_and_disk(self, transactions_df):
        result = persist_with_level(transactions_df, "MEMORY_AND_DISK")
        assert result.is_cached
        result.unpersist()


class TestMedium:
    def test_measure_cache_benefit_returns_dict(self, spark, transactions_df):
        result = measure_cache_benefit(spark, transactions_df)
        assert isinstance(result, dict)

    def test_measure_cache_benefit_keys(self, spark, transactions_df):
        result = measure_cache_benefit(spark, transactions_df)
        assert "without_cache_1" in result
        assert "without_cache_2" in result
        assert "with_cache_1" in result
        assert "with_cache_2" in result

    def test_measure_cache_benefit_positive_times(self, spark, transactions_df):
        result = measure_cache_benefit(spark, transactions_df)
        for key, val in result.items():
            assert val >= 0, f"{key} should be non-negative"


class TestHard:
    def test_iterative_with_checkpoint_columns(self, spark, transactions_df):
        result = iterative_with_checkpoint(spark, transactions_df, 6)
        assert "iter_1" in result.columns
        assert "iter_6" in result.columns

    def test_iterative_with_checkpoint_data_correct(self, spark, transactions_df):
        result = iterative_with_checkpoint(spark, transactions_df, 3)
        first_row = result.filter("tx_id = 'T001'").first()
        assert abs(first_row["iter_1"] - 1500.0 * 1) < 0.01
        assert abs(first_row["iter_2"] - 1500.0 * 2) < 0.01
        assert abs(first_row["iter_3"] - 1500.0 * 3) < 0.01

    def test_iterative_with_checkpoint_row_count(self, spark, transactions_df):
        result = iterative_with_checkpoint(spark, transactions_df, 5)
        assert result.count() == transactions_df.count()
