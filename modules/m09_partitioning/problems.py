"""
M09: 파티셔닝
도메인: 대규모 로그 / 주문 데이터
데이터: Faker 10만+ 행
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def adjust_partitions(df: DataFrame, n: int, use_coalesce: bool = False) -> DataFrame:
    """
    [Easy] repartition / coalesce로 파티션 수 조정

    use_coalesce=False이면 repartition(n),
    use_coalesce=True이면 coalesce(n)을 사용합니다.

    Args:
        df: 입력 DataFrame
        n: 목표 파티션 수
        use_coalesce: True면 coalesce, False면 repartition

    Returns:
        파티션 수가 n으로 조정된 DataFrame

    Hint:
        - df.repartition(n): 셔플 발생, 파티션 수 증가/감소 모두 가능
        - df.coalesce(n): 셔플 없이 감소만 가능 (n < 현재 파티션 수일 때만 효과)
        - df.rdd.getNumPartitions(): 현재 파티션 수 확인
    """
    raise NotImplementedError


def save_partitioned(df: DataFrame, path: str, col: str) -> None:
    """
    [Easy] partitionBy로 특정 컬럼 기준 Parquet 저장

    지정된 컬럼을 파티션 키로 사용하여 Parquet 파일로 저장합니다.
    디렉토리 구조: path/col=value1/, path/col=value2/, ...

    Args:
        df: 저장할 DataFrame
        path: 저장 경로
        col: 파티션 키 컬럼명

    Returns:
        None (파일 저장만 수행)

    Hint:
        - df.write.partitionBy(col).parquet(path)
        - mode="overwrite"로 기존 파일 덮어쓰기
    """
    raise NotImplementedError


def analyze_partition_skew(df: DataFrame, col: str) -> DataFrame:
    """
    [Medium] 파티션 키별 데이터 분포(skew) 분석

    지정된 컬럼의 값별 행 수를 집계하여 데이터 쏠림을 분석합니다.
    상위 값이 전체 데이터의 많은 비율을 차지하면 skew가 있습니다.

    Args:
        df: 분석할 DataFrame
        col: 분포를 확인할 컬럼명

    Returns:
        col, count, percentage 컬럼, count 내림차순 정렬
        percentage = count / total * 100

    Hint:
        - df.groupBy(col).count()
        - total = df.count()
        - withColumn("percentage", col("count") / total * 100)
        - from pyspark.sql.functions import col as spark_col
    """
    raise NotImplementedError


def salt_skewed_key(df: DataFrame, skew_col: str, num_salts: int) -> DataFrame:
    """
    [Hard] salting으로 데이터 스큐 해소

    skewed_col의 값에 랜덤 salt(0 ~ num_salts-1)를 붙여
    새로운 salted_key 컬럼을 생성합니다.
    이렇게 하면 동일한 키가 여러 파티션으로 분산됩니다.

    Args:
        df: 스큐가 있는 DataFrame
        skew_col: 스큐된 키 컬럼명
        num_salts: salt 개수 (파티션으로 분산할 수)

    Returns:
        salted_key 컬럼이 추가된 DataFrame
        salted_key 형식: f"{original_value}_{random_salt}"
        예: "Electronics_3", "Electronics_0"

    Hint:
        - from pyspark.sql.functions import concat, lit, col, (rand() * num_salts).cast("int")
        - salted_key = concat(col(skew_col), lit("_"), (rand() * num_salts).cast("int").cast("string"))
        - df.withColumn("salted_key", salted_key)
    """
    raise NotImplementedError
