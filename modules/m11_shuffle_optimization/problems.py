"""
M11: 셔플 최적화
도메인: 대규모 조인
데이터: Faker (큰 팩트 + 작은 룩업)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def broadcast_join(large_df: DataFrame, small_df: DataFrame, key: str) -> DataFrame:
    """
    [Easy] broadcast join으로 셔플 없이 조인

    small_df를 broadcast하여 셔플 없이 large_df와 조인합니다.
    broadcast join은 작은 테이블을 모든 executur에 복사하여
    셔플 없이 조인을 수행합니다.

    Args:
        large_df: 큰 팩트 테이블
        small_df: 작은 룩업 테이블 (broadcast 대상)
        key: 조인 키 컬럼명

    Returns:
        조인 결과 DataFrame

    Hint:
        - from pyspark.sql.functions import broadcast
        - large_df.join(broadcast(small_df), on=key, how="inner")
    """
    raise NotImplementedError


def optimize_shuffle_partitions(spark: SparkSession, df: DataFrame, query_func) -> dict:
    """
    [Medium] 셔플 파티션 수 조정 비교

    spark.sql.shuffle.partitions를 다른 값으로 설정하면서
    같은 쿼리(query_func)의 실행 시간을 비교합니다.

    Args:
        spark: SparkSession
        df: 테스트 DataFrame
        query_func: df를 받아 집계를 수행하는 함수

    Returns:
        {"partitions_4": float, "partitions_200": float} (초 단위)

    Hint:
        - spark.conf.set("spark.sql.shuffle.partitions", "4")
        - import time; t = time.time(); query_func(df).collect(); elapsed = time.time() - t
        - 테스트 후 원래 값으로 복원
    """
    raise NotImplementedError


def pre_partitioned_joins(df1: DataFrame, df2: DataFrame, key: str, n: int) -> DataFrame:
    """
    [Medium] 사전 파티셔닝으로 반복 join 최적화

    두 DataFrame을 key 기준으로 미리 repartition한 후 join합니다.
    같은 키를 같은 파티션에 모아 조인 셔플을 최소화합니다.

    Args:
        df1: 첫 번째 DataFrame
        df2: 두 번째 DataFrame
        key: 조인 키 컬럼명
        n: 파티션 수

    Returns:
        사전 파티셔닝 후 조인된 DataFrame

    Hint:
        - df1.repartition(n, key): key 컬럼 기준으로 n개 파티션
        - df2.repartition(n, key): 같은 기준으로 파티셔닝
        - 이후 join 시 같은 키가 같은 파티션에 → 추가 셔플 없이 조인
    """
    raise NotImplementedError


def handle_skew_join(skewed_df: DataFrame, normal_df: DataFrame, key: str) -> DataFrame:
    """
    [Hard] 스큐 조인 최적화 (salting)

    skewed_df의 key 컬럼이 특정 값에 집중된 경우,
    salting을 적용하여 스큐 조인을 최적화합니다.

    전략:
    1. skewed_df: key에 salt(0~3) 추가 → salted_key
    2. normal_df: 각 행을 4번 복제하여 salt 0~3 추가 → salted_key
    3. salted_key로 조인
    4. 조인 결과에서 salted_key 제거

    Args:
        skewed_df: 스큐가 있는 DataFrame (key 컬럼)
        normal_df: 정상 DataFrame (key 컬럼)
        key: 조인 키 컬럼명

    Returns:
        조인 결과 DataFrame (salted_key 컬럼 없음)

    Hint:
        - from pyspark.sql.functions import concat, lit, col, rand, explode, array
        - skewed_df: withColumn("salted_key", concat(col(key), lit("_"), (rand()*4).cast("int").cast("string")))
        - normal_df: withColumn("salt", explode(array([lit(i) for i in range(4)])))
                     .withColumn("salted_key", concat(col(key), lit("_"), col("salt").cast("string")))
        - join on "salted_key"
        - drop("salted_key", "salt")
    """
    raise NotImplementedError
