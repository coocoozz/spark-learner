"""
M07: UDF (User Defined Functions)
도메인: 이커머스 텍스트
데이터: Faker 생성
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def register_normalize_udf(spark: SparkSession):
    """
    [Easy] 문자열 정규화 UDF 등록 및 반환

    텍스트를 소문자로 변환하고, 앞뒤 공백을 제거하고,
    연속된 공백을 단일 공백으로 치환하는 UDF를 반환합니다.

    Args:
        spark: SparkSession

    Returns:
        정규화 UDF 함수 (StringType 반환)

    사용 예시:
        normalize = register_normalize_udf(spark)
        df.withColumn("normalized", normalize(col("text")))

    Hint:
        - from pyspark.sql.functions import udf
        - from pyspark.sql.types import StringType
        - import re
        - udf(lambda text: ..., StringType())
        - 또는 @udf(returnType=StringType()) 데코레이터
    """
    # TODO: 구현하세요
    raise NotImplementedError


def register_address_parser_udf(spark: SparkSession):
    """
    [Medium] 주소 파싱 UDF (구조체 반환)

    주소 문자열에서 우편번호를 파싱하여 구조체로 반환하는 UDF를 반환합니다.
    우편번호: 5자리 숫자 패턴

    반환 구조체:
    - original: 원본 주소
    - zip_code: 파싱된 우편번호 (없으면 "UNKNOWN")

    Args:
        spark: SparkSession

    Returns:
        StructType을 반환하는 UDF

    Hint:
        - from pyspark.sql.types import StructType, StructField, StringType
        - 반환 타입: StructType([StructField("original", StringType()), StructField("zip_code", StringType())])
        - import re; re.search(r'\d{5}', address)
        - udf(func, return_schema)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def register_sentiment_udf(spark: SparkSession):
    """
    [Medium] 감성 점수 UDF

    리뷰 텍스트와 숫자 점수(1-5)를 기반으로 감성 레이블을 반환합니다:
    - score >= 4: "positive"
    - score == 3: "neutral"
    - score <= 2: "negative"

    Args:
        spark: SparkSession

    Returns:
        (text, score) -> sentiment_label UDF

    Hint:
        - udf(lambda score: "positive" if score >= 4 else ..., StringType())
        - score가 None이면 "unknown" 반환
    """
    # TODO: 구현하세요
    raise NotImplementedError


def normalize_by_category(df: DataFrame, col_name: str) -> DataFrame:
    """
    [Hard] Pandas UDF - 카테고리별 Z-score 정규화

    각 카테고리 내에서 지정된 컬럼의 Z-score를 계산합니다.
    Z-score = (x - mean) / std

    Args:
        df: category 컬럼과 col_name 컬럼을 포함하는 DataFrame
        col_name: Z-score를 계산할 수치 컬럼명

    Returns:
        f"{col_name}_zscore" 컬럼이 추가된 DataFrame

    Hint:
        - from pyspark.sql.functions import pandas_udf
        - from pyspark.sql.types import DoubleType
        - @pandas_udf(DoubleType())
          def zscore_udf(series: pd.Series) -> pd.Series:
              mean = series.mean()
              std = series.std()
              return (series - mean) / std if std > 0 else pd.Series([0.0] * len(series))
        - df.groupBy("category").applyInPandas(func, schema) 또는
          df.withColumn("zscore", zscore_udf(col_name).over(Window.partitionBy("category")))
    """
    # TODO: 구현하세요
    raise NotImplementedError
