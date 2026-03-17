"""
M10: 캐싱과 영속성
도메인: 금융 거래
데이터: Faker 생성
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def cache_and_verify(df: DataFrame) -> tuple[DataFrame, bool, bool]:
    """
    [Easy] cache + isCached 확인 후 unpersist

    DataFrame을 캐시하고 캐시 여부를 확인한 다음,
    unpersist 후 다시 확인합니다.

    Args:
        df: 캐시할 DataFrame

    Returns:
        (cached_df, is_cached_before_unpersist, is_cached_after_unpersist) 튜플
        - cached_df: 캐시된 DataFrame
        - is_cached_before_unpersist: True여야 함
        - is_cached_after_unpersist: False여야 함

    Hint:
        - df.cache(): DataFrame을 메모리에 캐시 (기본 MEMORY_AND_DISK)
        - df.count(): 캐시를 실제로 실행하려면 Action 필요
        - df.is_cached: 캐시 여부 (property)
        - df.unpersist(): 캐시 해제
    """
    raise NotImplementedError


def persist_with_level(df: DataFrame, level: str) -> DataFrame:
    """
    [Easy] 지정된 StorageLevel로 persist

    Args:
        df: persist할 DataFrame
        level: StorageLevel 문자열
               "MEMORY_ONLY", "MEMORY_AND_DISK", "DISK_ONLY",
               "MEMORY_ONLY_SER", "MEMORY_AND_DISK_SER" 중 하나

    Returns:
        지정된 StorageLevel로 persist된 DataFrame

    Hint:
        - from pyspark import StorageLevel
        - StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_AND_DISK 등
        - df.persist(StorageLevel.MEMORY_ONLY)
        - level 문자열을 StorageLevel 객체로 변환해야 합니다
    """
    raise NotImplementedError


def measure_cache_benefit(spark: SparkSession, df: DataFrame) -> dict:
    """
    [Medium] cache 유무 성능 비교

    같은 쿼리를 캐시 없이 2번, 캐시 후 2번 실행하여
    실행 시간을 비교합니다.

    Args:
        spark: SparkSession
        df: 테스트할 DataFrame

    Returns:
        {
            "without_cache_1": float (초),
            "without_cache_2": float (초),
            "with_cache_1": float (초),
            "with_cache_2": float (초),
        }

    Hint:
        - import time; start = time.time(); ...; elapsed = time.time() - start
        - df.groupBy("category").count().collect() 같은 집계를 반복
        - df.cache() 후 Action 실행 → 두 번째는 캐시에서 읽음
        - df.unpersist() 로 정리
    """
    raise NotImplementedError


def iterative_with_checkpoint(spark: SparkSession, df: DataFrame, iterations: int) -> DataFrame:
    """
    [Hard] checkpoint로 lineage 차단

    반복적인 변환(iterations 횟수만큼 새 컬럼 추가)을 수행하는 과정에서
    일정 주기마다 checkpoint를 수행하여 lineage를 차단합니다.

    Args:
        spark: SparkSession
        df: 입력 DataFrame (amount 컬럼 포함)
        iterations: 변환 반복 횟수

    Returns:
        iterations만큼 변환이 적용된 DataFrame

    Hint:
        - spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
        - 매 5회마다 df.checkpoint() 수행
        - df.checkpoint()는 Action + Transformation: DataFrame을 저장하고 새 DF 반환
        - 변환 예: df.withColumn(f"iter_{i}", col("amount") * i)
    """
    raise NotImplementedError
