"""
M10: 캐싱과 영속성 - 솔루션
"""
from __future__ import annotations

import time

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def cache_and_verify(df: DataFrame) -> tuple[DataFrame, bool, bool]:
    """
    풀이 설명:

    cache()는 persist(StorageLevel.MEMORY_AND_DISK)의 단축형입니다.

    Lazy Evaluation과 캐시:
    - cache()를 호출해도 즉시 실행되지 않습니다.
    - Action(count, collect 등)이 실행될 때 비로소 캐시됩니다.
    - 이를 "materialization"이라고 합니다.

    is_cached property:
    - True: persist/cache 호출 후 (아직 Action 전에도 True)
    - False: unpersist 후

    unpersist(blocking=True):
    - blocking=True: 블록 제거가 완료될 때까지 대기
    - blocking=False (기본값): 비동기 해제
    """
    cached_df = df.cache()
    cached_df.count()  # Action으로 실제 캐시 수행
    is_cached_before = cached_df.is_cached
    cached_df.unpersist()
    is_cached_after = cached_df.is_cached
    return (cached_df, is_cached_before, is_cached_after)


def persist_with_level(df: DataFrame, level: str) -> DataFrame:
    """
    풀이 설명:

    StorageLevel 종류:
    | Level              | 메모리 | 디스크 | 직렬화 | 복제 |
    |--------------------|--------|--------|--------|------|
    | MEMORY_ONLY        | O      | X      | X      | 1    |
    | MEMORY_AND_DISK    | O      | O      | X      | 1    |
    | DISK_ONLY          | X      | O      | X      | 1    |
    | MEMORY_ONLY_SER    | O(직) | X      | O      | 1    |
    | MEMORY_AND_DISK_SER| O(직) | O      | O      | 1    |
    | MEMORY_ONLY_2      | O      | X      | X      | 2    |

    선택 기준:
    - 메모리 충분: MEMORY_ONLY (가장 빠름)
    - 메모리 부족: MEMORY_AND_DISK
    - 네트워크 병목: MEMORY_ONLY_SER (직렬화로 크기 감소)
    - 장애 내성: _2 suffix (복제본 2개)
    """
    level_map = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
        "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
    }
    storage_level = level_map.get(level, StorageLevel.MEMORY_AND_DISK)
    return df.persist(storage_level)


def measure_cache_benefit(spark: SparkSession, df: DataFrame) -> dict:
    """
    풀이 설명:

    캐시 효과가 나타나는 조건:
    1. 같은 DataFrame을 여러 번 재사용할 때
    2. 계산 비용이 높은 변환 후 결과를 재사용할 때
    3. 반복적인 머신러닝 학습 (Spark MLlib)

    캐시를 사용하지 않는 것이 나을 때:
    1. DataFrame을 한 번만 사용할 때
    2. 데이터가 메모리에 비해 너무 클 때
    3. 재계산이 디스크 읽기보다 빠를 때

    측정 방법:
    - time.time()으로 실행 시간 측정
    - Action(collect, count)이 실행되어야 실제 측정 가능
    """
    def run_query():
        return df.groupBy("category").agg({"amount": "sum"}).collect()

    # 캐시 없이 2번 실행
    t1 = time.time(); run_query(); without_1 = time.time() - t1
    t1 = time.time(); run_query(); without_2 = time.time() - t1

    # 캐시 후 2번 실행
    df.cache()
    df.count()  # materialize
    t1 = time.time(); run_query(); with_1 = time.time() - t1
    t1 = time.time(); run_query(); with_2 = time.time() - t1
    df.unpersist()

    return {
        "without_cache_1": without_1,
        "without_cache_2": without_2,
        "with_cache_1": with_1,
        "with_cache_2": with_2,
    }


def iterative_with_checkpoint(spark: SparkSession, df: DataFrame, iterations: int) -> DataFrame:
    """
    풀이 설명:

    Lineage(계보) 문제:
    - 각 Transformation은 이전 RDD/DF에 대한 참조를 유지합니다.
    - 반복적인 변환이 쌓이면 lineage가 매우 길어져:
      1. StackOverflow 발생 가능
      2. 장애 복구 시 처음부터 재계산

    checkpoint()로 해결:
    - 현재 상태를 디스크에 저장하고 새 DataFrame 반환
    - 이전 lineage는 차단됨 (저장된 파일에서 시작)
    - Spark MLlib의 반복 알고리즘(ALS, 그래프 처리 등)에서 필수

    checkpoint vs persist:
    - persist: lineage 유지, 메모리/디스크에 캐시
    - checkpoint: lineage 차단, 디스크에만 저장

    체크포인트 주기 = 5: 5번마다 한 번 lineage 차단
    """
    import tempfile
    checkpoint_dir = tempfile.mkdtemp(prefix="spark_checkpoint_")
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    result = df
    for i in range(1, iterations + 1):
        result = result.withColumn(f"iter_{i}", col("amount") * i)
        if i % 5 == 0:
            result = result.checkpoint()

    return result
