"""
M03: DataFrame 기초 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def load_and_describe(spark: SparkSession, path: str) -> tuple[DataFrame, int, int]:
    """
    풀이 설명:

    inferSchema=True: Spark가 데이터를 샘플링하여 타입을 자동 추론합니다.
    - 장점: 편리함
    - 단점: 추가 스캔 비용, 프로덕션에서는 명시적 스키마 권장

    header=True: 첫 번째 줄을 컬럼명으로 사용합니다.

    df.count()는 Action으로 즉시 실행됩니다.
    len(df.columns)는 스키마 메타데이터를 읽으므로 Action이 아닙니다.
    """
    df = spark.read.csv(path, header=True, inferSchema=True)
    return (df, df.count(), len(df.columns))


def load_with_schema(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    풀이 설명:

    명시적 스키마 지정의 장점:
    1. 성능: inferSchema의 추가 스캔 없음
    2. 타입 안정성: 예상치 못한 타입 변환 방지
    3. 빈 파일 처리: inferSchema는 빈 파일에서 실패

    JSON Lines 형식 (기본값 multiLine=False):
    각 줄이 독립적인 JSON 객체입니다.
    {"id": 1, "name": "Alice"}
    {"id": 2, "name": "Bob"}
    """
    return spark.read.json(path, schema=schema)


def create_from_dicts(spark: SparkSession, data: list[dict], schema: StructType) -> DataFrame:
    """
    풀이 설명:

    createDataFrame은 다양한 소스로부터 DataFrame을 생성합니다:
    - list of dicts: 각 dict가 한 행
    - list of tuples: 컬럼 순서대로 값
    - list of Row: Row 객체
    - Pandas DataFrame

    스키마 명시의 중요성:
    - 스키마 없이 dict 리스트를 주면 타입 추론이 불확실합니다.
    - 예: 정수 100이 LongType으로 추론될 수 있습니다.
    """
    return spark.createDataFrame(data, schema=schema)


def safe_union(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    풀이 설명:

    unionByName vs union:
    - union: 컬럼 순서 기준으로 합치기 (이름 다를 수 있음)
    - unionByName: 컬럼 이름 기준으로 합치기 (순서 무관)
    → 안전한 결합에는 unionByName을 사용합니다.

    cast 타입 문자열:
    - "double", "float", "integer", "long", "string", "boolean", "date" 등
    - DoubleType() 인스턴스로도 가능

    allowMissingColumns=True: 한 쪽에 없는 컬럼은 null로 채웁니다.
    """
    df1_cast = df1.withColumn("amount", col("amount").cast("double"))
    df2_cast = df2.withColumn("amount", col("amount").cast("double"))
    return df1_cast.unionByName(df2_cast)


def flatten_nested(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    중첩 StructType 컬럼을 평탄화하는 일반적인 패턴:
    1. 스키마를 순회하여 StructType 컬럼을 찾습니다.
    2. 각 하위 필드를 "parent_child" 형식으로 select합니다.

    col("address.street"): 중첩 필드 접근은 점(.) 표기법 사용
    .alias("address_street"): 컬럼명 변경

    재귀적 중첩 처리가 필요하면 재귀 함수로 구현합니다.
    이 솔루션은 1단계 중첩만 처리합니다.
    """
    select_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for sub_field in field.dataType.fields:
                select_cols.append(
                    col(f"{field.name}.{sub_field.name}").alias(f"{field.name}_{sub_field.name}")
                )
        else:
            select_cols.append(col(field.name))
    return df.select(*select_cols)
