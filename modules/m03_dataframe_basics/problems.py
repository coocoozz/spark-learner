"""
M03: DataFrame 기초
도메인: 이커머스
데이터: data/ecommerce/ (CSV/JSON 파일)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def load_and_describe(spark: SparkSession, path: str) -> tuple[DataFrame, int, int]:
    """
    [Easy] CSV 파일 읽기 + 스키마/크기 확인

    CSV 파일을 읽어 DataFrame을 만들고, 행 수와 컬럼 수를 반환합니다.
    header=True, inferSchema=True 옵션을 사용합니다.

    Args:
        spark: SparkSession
        path: CSV 파일 경로

    Returns:
        (df, row_count, col_count) 튜플

    Hint:
        - spark.read.csv(path, header=True, inferSchema=True)
        - df.count(): 행 수
        - len(df.columns): 컬럼 수
    """
    # TODO: 구현하세요
    raise NotImplementedError


def load_with_schema(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    [Easy] 명시적 StructType으로 JSON 읽기

    JSON Lines 형식의 파일을 명시적 스키마로 읽습니다.
    스키마를 미리 지정하면 inferSchema 비용을 절약하고 타입을 보장합니다.

    Args:
        spark: SparkSession
        path: JSON 파일 경로
        schema: StructType 스키마 정의

    Returns:
        지정된 스키마로 읽은 DataFrame

    Hint:
        - spark.read.json(path, schema=schema)
        - multiLine=False가 기본값 (JSON Lines 형식)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def create_from_dicts(spark: SparkSession, data: list[dict], schema: StructType) -> DataFrame:
    """
    [Medium] Python dict 리스트에서 DataFrame 생성

    Python 딕셔너리 리스트를 Spark DataFrame으로 변환합니다.
    스키마를 명시적으로 지정하여 타입 안정성을 보장합니다.

    Args:
        spark: SparkSession
        data: 딕셔너리 리스트 [{"col1": val1, "col2": val2}, ...]
        schema: StructType 스키마

    Returns:
        생성된 DataFrame

    Hint:
        - spark.createDataFrame(data, schema=schema)
        - Row 객체로 변환도 가능: [Row(**d) for d in data]
    """
    # TODO: 구현하세요
    raise NotImplementedError


def safe_union(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    [Medium] 컬럼 타입 캐스팅 + unionByName

    두 DataFrame의 특정 컬럼 타입이 다를 수 있습니다.
    amount 컬럼을 DoubleType으로 통일한 후 unionByName으로 합칩니다.

    Args:
        df1: 첫 번째 DataFrame (amount 컬럼이 StringType일 수 있음)
        df2: 두 번째 DataFrame

    Returns:
        타입 통일 후 합친 DataFrame

    Hint:
        - df.withColumn("amount", col("amount").cast("double"))
        - df1.unionByName(df2): 컬럼 이름 기준으로 합치기 (순서 무관)
        - from pyspark.sql.functions import col
    """
    # TODO: 구현하세요
    raise NotImplementedError


def flatten_nested(df: DataFrame) -> DataFrame:
    """
    [Hard] 중첩 JSON 평탄화

    중첩된 StructType 컬럼을 최상위 컬럼으로 평탄화합니다.
    예: address.street, address.zip → address_street, address_zip

    customers.json의 address 필드처럼 중첩된 구조를 처리합니다.

    Args:
        df: 중첩 컬럼을 가진 DataFrame
            예: customer_id, name, address(StructType{street, zip})

    Returns:
        중첩 제거된 평탄화된 DataFrame
        address.street → address_street
        address.zip → address_zip

    Hint:
        - df.schema로 컬럼 타입 확인
        - StructType 컬럼: col("address.street").alias("address_street")
        - select()로 모든 컬럼을 한 번에 선택
        - isinstance(field.dataType, StructType) 로 중첩 여부 확인
    """
    # TODO: 구현하세요
    raise NotImplementedError
