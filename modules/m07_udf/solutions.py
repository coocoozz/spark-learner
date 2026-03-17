"""
M07: UDF - 솔루션
"""
from __future__ import annotations

import re

import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window


def register_normalize_udf(spark: SparkSession):
    """
    풀이 설명:

    UDF (User Defined Function) 등록 방법:
    1. udf(func, returnType): 함수와 반환 타입을 명시
    2. @udf(returnType=...) 데코레이터
    3. spark.udf.register("name", func, returnType): SQL에서 사용 가능

    주의: UDF는 Catalyst Optimizer의 최적화를 받지 못합니다.
    가능하면 내장 함수(lower, trim, regexp_replace)를 사용하세요.
    이 예제는 학습 목적입니다.

    내장 함수로 대체:
    from pyspark.sql.functions import lower, trim, regexp_replace
    df.withColumn("norm", trim(lower(regexp_replace(col("text"), r"\\s+", " "))))
    """
    def normalize(text: str) -> str:
        if text is None:
            return None
        return re.sub(r'\s+', ' ', text.strip().lower())

    return udf(normalize, StringType())


def register_address_parser_udf(spark: SparkSession):
    """
    풀이 설명:

    구조체(StructType) 반환 UDF:
    - Python 함수가 dict 또는 Row를 반환하면 StructType으로 매핑됩니다.
    - 반환 타입을 명확히 StructType으로 지정해야 합니다.

    사용 시:
        parser = register_address_parser_udf(spark)
        df.withColumn("parsed", parser(col("address_raw")))
          .withColumn("zip", col("parsed.zip_code"))

    dict 반환: {"original": ..., "zip_code": ...}
    필드 접근: col("parsed.zip_code")
    """
    return_schema = StructType([
        StructField("original", StringType(), True),
        StructField("zip_code", StringType(), True),
    ])

    def parse_address(address: str) -> dict:
        if address is None:
            return {"original": None, "zip_code": "UNKNOWN"}
        match = re.search(r'\d{5}', address)
        zip_code = match.group(0) if match else "UNKNOWN"
        return {"original": address, "zip_code": zip_code}

    return udf(parse_address, return_schema)


def register_sentiment_udf(spark: SparkSession):
    """
    풀이 설명:

    단순 조건 분기 UDF:
    - Python 함수에서 score 값에 따라 문자열 레이블 반환
    - null 안전 처리: score가 None이면 "unknown" 반환

    성능 팁:
    내장 함수로 대체 가능:
    from pyspark.sql.functions import when
    when(col("score") >= 4, "positive")
    .when(col("score") == 3, "neutral")
    .when(col("score") <= 2, "negative")
    .otherwise("unknown")
    """
    def sentiment(score) -> str:
        if score is None:
            return "unknown"
        if score >= 4:
            return "positive"
        elif score == 3:
            return "neutral"
        else:
            return "negative"

    return udf(sentiment, StringType())


def normalize_by_category(df: DataFrame, col_name: str) -> DataFrame:
    """
    풀이 설명:

    Pandas UDF (Vectorized UDF):
    - 일반 UDF는 행 단위 처리 → Python 호출 오버헤드 큼
    - Pandas UDF는 청크(배치) 단위 처리 → Apache Arrow 활용, 10-100x 빠름

    pandas_udf(returnType):
    - Series → Series: 행별 변환
    - GroupedData.applyInPandas: 그룹 전체를 Pandas로 처리

    Window를 사용하는 방법:
    - partitionBy("category")로 카테고리별 Z-score 계산
    - pandas_udf는 Window.partitionBy()와 함께 사용 가능

    Z-score = (x - mean) / std
    std = 0이면 0으로 처리 (ZeroDivisionError 방지)
    """
    @pandas_udf(DoubleType())
    def zscore_udf(series: pd.Series) -> pd.Series:
        mean = series.mean()
        std = series.std()
        if std == 0 or pd.isna(std):
            return pd.Series([0.0] * len(series))
        return (series - mean) / std

    w = Window.partitionBy("category")
    return df.withColumn(f"{col_name}_zscore", zscore_udf(col(col_name)).over(w))
