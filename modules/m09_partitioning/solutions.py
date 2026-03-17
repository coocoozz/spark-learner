"""
M09: 파티셔닝 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, rand


def adjust_partitions(df: DataFrame, n: int, use_coalesce: bool = False) -> DataFrame:
    """
    풀이 설명:

    repartition(n):
    - 셔플(Shuffle) 발생: 모든 데이터를 네트워크로 재분배
    - 파티션 수 증가/감소 모두 가능
    - 데이터를 균등하게 분배

    coalesce(n):
    - 셔플 없이 파티션 병합: 인접 파티션들을 합침
    - 파티션 수를 줄이는 것만 가능 (늘리면 repartition으로 폴백)
    - 데이터 불균등 가능성 있음

    언제 사용?
    - 파티션 수를 줄일 때: coalesce (셔플 비용 절감)
    - 파티션 수를 늘리거나 균등화할 때: repartition
    - 저장 전 파티션 수 최적화: coalesce(적당한 수)
    """
    if use_coalesce:
        return df.coalesce(n)
    return df.repartition(n)


def save_partitioned(df: DataFrame, path: str, col: str) -> None:
    """
    풀이 설명:

    partitionBy(col)로 저장하면:
    path/
    ├── category=Electronics/
    │   └── part-00000.parquet
    ├── category=Clothing/
    │   └── part-00000.parquet
    └── ...

    장점:
    - 쿼리 시 특정 파티션만 읽음 (Partition Pruning)
    - category='Electronics' 필터 → Electronics 디렉토리만 스캔

    주의:
    - 카디널리티가 높은 컬럼 (예: order_id)은 파티션 키 부적합
    - 파일이 너무 많이 생성됨 (small file problem)
    """
    df.write.partitionBy(col).mode("overwrite").parquet(path)


def analyze_partition_skew(df: DataFrame, col_name: str) -> DataFrame:
    """
    풀이 설명:

    스큐(Skew) 분석:
    - 특정 키에 데이터가 집중되면 해당 파티션의 태스크가 느려짐
    - 전체 작업이 가장 느린 파티션에 의해 결정됨 (Straggler)

    percentage 계산:
    - total을 Python 변수로 collect 후 withColumn에서 사용
    - broadcast 없이도 스칼라 값으로 사용 가능

    스큐 판단 기준:
    - 상위 1개 값이 전체의 > 30%: 스큐 의심
    - 상위 1개 값이 전체의 > 50%: 심각한 스큐
    """
    from pyspark.sql.functions import col as spark_col
    total = df.count()
    return (
        df.groupBy(col_name)
        .count()
        .withColumn("percentage", spark_col("count") / total * 100)
        .orderBy("count", ascending=False)
    )


def salt_skewed_key(df: DataFrame, skew_col: str, num_salts: int) -> DataFrame:
    """
    풀이 설명:

    Salting 기법:
    "Electronics" 키가 70%를 차지하는 경우:
    - 원래: 모든 Electronics 주문이 같은 파티션 → 병목
    - Salting 후: "Electronics_0", "Electronics_1", ... "Electronics_9" →
      10개 파티션으로 분산

    rand(): 0~1 사이 균등 분포 난수
    (rand() * num_salts).cast("int"): 0 ~ num_salts-1 정수

    concat(col(skew_col), lit("_"), salt):
    - lit("_"): 리터럴 문자열 컬럼
    - 문자열 연결

    주의: Salting 후 조인 시 상대 테이블도 explode로 salt 범위를 확장해야 합니다.

    AQE(Adaptive Query Execution)의 Skew Join 최적화:
    Spark 3.0+에서는 AQE가 자동으로 스큐 조인을 감지하고 분할합니다.
    spark.sql.adaptive.enabled=true (기본값)
    """
    salt = (rand() * num_salts).cast("int").cast("string")
    salted_key = concat(col(skew_col), lit("_"), salt)
    return df.withColumn("salted_key", salted_key)
