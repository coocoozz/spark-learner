"""
M11: 셔플 최적화 - 솔루션
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, broadcast, col, concat, explode, lit, rand


def broadcast_join(large_df: DataFrame, small_df: DataFrame, key: str) -> DataFrame:
    """
    풀이 설명:

    Broadcast Join 동작 원리:
    1. small_df를 드라이버가 수집
    2. 모든 executor에 브로드캐스트 (네트워크 전송)
    3. 각 executor에서 로컬 해시 조인 수행 → 셔플 없음

    vs Shuffle Hash Join / Sort Merge Join:
    - 셔플 해시 조인: 양쪽 모두 셔플 후 파티션별 해시 조인
    - Sort Merge Join: 양쪽 정렬 후 병합 (기본 전략)
    - Broadcast Join: 셔플 없음 (가장 빠름, 작은 테이블에만 적용 가능)

    자동 broadcast:
    - spark.sql.autoBroadcastJoinThreshold = 10MB (기본)
    - 이 크기 이하면 Spark가 자동으로 broadcast 적용

    명시적 broadcast 힌트:
    - broadcast(df): 항상 broadcast 사용
    - df.hint("broadcast"): SQL 힌트
    """
    return large_df.join(broadcast(small_df), on=key, how="inner")


def optimize_shuffle_partitions(spark: SparkSession, df: DataFrame, query_func) -> dict:
    """
    풀이 설명:

    spark.sql.shuffle.partitions:
    - 기본값: 200
    - 집계/조인 후 생성되는 파티션 수 결정
    - 데이터가 작으면 200 파티션은 과도 → 태스크 오버헤드

    적정 파티션 수:
    - 일반 기준: 코어 수 * 2~4
    - 데이터 크기 기준: 파티션당 100~200MB
    - 너무 적으면: 파티션당 데이터 과부하, 메모리 부족
    - 너무 많으면: 태스크 스케줄링 오버헤드, 소형 파일 문제

    AQE(Adaptive Query Execution):
    - spark.sql.adaptive.enabled=true (기본값 Spark 3.0+)
    - 실행 중 파티션 수를 동적으로 조정
    - coalescePartitions: 빈/작은 파티션 자동 병합
    """
    original = spark.conf.get("spark.sql.shuffle.partitions")

    spark.conf.set("spark.sql.shuffle.partitions", "4")
    t = time.time(); query_func(df).collect(); time_4 = time.time() - t

    spark.conf.set("spark.sql.shuffle.partitions", "200")
    t = time.time(); query_func(df).collect(); time_200 = time.time() - t

    spark.conf.set("spark.sql.shuffle.partitions", original)
    return {"partitions_4": time_4, "partitions_200": time_200}


def pre_partitioned_joins(df1: DataFrame, df2: DataFrame, key: str, n: int) -> DataFrame:
    """
    풀이 설명:

    사전 파티셔닝(Pre-partitioning) 전략:
    1. 두 DataFrame을 같은 기준(키, 파티션 수)으로 repartition
    2. 조인 시 같은 키가 같은 파티션에 위치
    3. Spark가 추가 셔플 없이 파티션별 로컬 조인 수행

    repartition(n, col):
    - 컬럼 값의 해시로 파티션 결정
    - 같은 키 = 같은 해시 = 같은 파티션 (양쪽 DF 모두)

    반복 조인에서 효과:
    - 같은 df를 여러 DF와 조인할 때 한 번만 파티셔닝
    - bucket join(테이블에 저장 시)과 유사한 효과
    """
    df1_part = df1.repartition(n, key)
    df2_part = df2.repartition(n, key)
    return df1_part.join(df2_part, on=key, how="inner")


def handle_skew_join(skewed_df: DataFrame, normal_df: DataFrame, key: str) -> DataFrame:
    """
    풀이 설명:

    스큐 조인 문제:
    - 특정 키 값이 전체 데이터의 많은 부분을 차지
    - 해당 키를 담은 파티션의 태스크가 극단적으로 느림
    - 나머지 태스크가 끝나도 스큐 파티션을 기다림 → 전체 지연

    Salting 솔루션:
    1. skewed_df: 각 행에 랜덤 salt(0~3) 추가
       "Electronics_0", "Electronics_1" 등으로 키 분산

    2. normal_df: 각 행을 4개로 복제 (salt 0,1,2,3)
       "Electronics_0", "Electronics_1", "Electronics_2", "Electronics_3"
       → skewed_df의 모든 salt와 매칭 가능

    3. salted_key로 조인 후 salt 관련 컬럼 제거

    explode(array([lit(0), lit(1), lit(2), lit(3)])):
    - array: [0, 1, 2, 3] 배열 컬럼 생성
    - explode: 배열의 각 원소를 별도 행으로 분리 (4배 행 증가)

    AQE Skew Join (Spark 3.0+):
    - spark.sql.adaptive.skewJoin.enabled=true
    - 자동으로 스큐 파티션을 감지하고 분할
    """
    num_salts = 4

    # skewed_df에 랜덤 salt 추가
    skewed_salted = skewed_df.withColumn(
        "salted_key",
        concat(col(key), lit("_"), (rand() * num_salts).cast("int").cast("string")),
    )

    # normal_df를 num_salts배 복제
    normal_exploded = (
        normal_df
        .withColumn("salt", explode(array([lit(i) for i in range(num_salts)])))
        .withColumn("salted_key", concat(col(key), lit("_"), col("salt").cast("string")))
    )

    return (
        skewed_salted
        .join(normal_exploded, on="salted_key", how="inner")
        .drop("salted_key", "salt")
    )
