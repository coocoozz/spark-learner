"""
M04: DataFrame 연산 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, datediff, regexp_extract, to_date, when


def add_total_column(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    withColumn(name, expression):
    - 새 컬럼을 추가하거나 기존 컬럼을 덮어씁니다
    - 컬럼 이름이 이미 있으면 덮어쓰기

    col("column_name") vs df["column_name"]:
    - 둘 다 컬럼을 참조하지만 col()이 더 유연합니다
    - 여러 DataFrame을 다룰 때 col()이 명확합니다

    산술 연산자: +, -, *, / 모두 컬럼 간에 직접 사용 가능
    """
    return df.withColumn("total", col("amount") * col("quantity"))


def filter_and_sort(df: DataFrame, min_amount: float, sort_col: str) -> DataFrame:
    """
    풀이 설명:

    filter vs where:
    - df.filter(condition): 조건에 맞는 행만 유지
    - df.where(condition): filter의 별칭, SQL 문법에 익숙한 사람에게 친숙

    SQL 문자열로도 사용 가능:
    - df.filter(f"amount >= {min_amount}")

    orderBy vs sort:
    - 동일한 기능, orderBy가 SQL 스타일
    - asc()/desc()로 정렬 방향 지정 가능
    """
    return df.filter(col("amount") >= min_amount).orderBy(sort_col)


def categorize_orders(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    when/otherwise는 SQL의 CASE WHEN과 동일합니다:
    CASE WHEN amount >= 500 THEN 'Premium'
         WHEN amount >= 100 THEN 'Standard'
         ELSE 'Basic'
    END

    조건 순서가 중요합니다:
    - 첫 번째 True 조건이 선택됩니다
    - amount=600은 첫 번째 조건(>=500)에서 True → 'Premium' 반환

    otherwise() 없이 모든 조건이 False면 null을 반환합니다.
    """
    order_tier = (
        when(col("amount") >= 500, "Premium")
        .when(col("amount") >= 100, "Standard")
        .otherwise("Basic")
    )
    return df.withColumn("order_tier", order_tier)


def calculate_delivery_days(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    날짜 처리 흐름:
    1. 문자열 → DateType: to_date(col, format)
    2. 날짜 차이 계산: datediff(end, start)

    날짜 형식 패턴 (Java SimpleDateFormat):
    - "yyyy-MM-dd": 2023-01-05
    - "dd/MM/yyyy": 05/01/2023
    - "MM/dd/yyyy HH:mm:ss": 01/05/2023 13:30:00

    datediff는 (end - start) 일수를 반환합니다.
    delivery_date > order_date면 양수, 아니면 음수입니다.
    """
    return df.withColumn(
        "delivery_days",
        datediff(
            to_date(col("delivery_date"), "yyyy-MM-dd"),
            to_date(col("order_date"), "yyyy-MM-dd"),
        ),
    )


def clean_address_column(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    regexp_extract(str, pattern, idx):
    - str: 검색할 컬럼
    - pattern: 정규식 패턴
    - idx: 캡처 그룹 번호 (0=전체 매치, 1=첫 번째 그룹)

    r"(\\d{5})": 연속 5자리 숫자를 캡처 그룹으로 감쌈
    - \\d: 숫자 문자 [0-9]
    - {5}: 정확히 5번 반복
    - (): 캡처 그룹 → idx=1로 참조

    매치 없을 때: 빈 문자열("") 반환
    (null이 아닌 빈 문자열이므로 주의)

    다른 방법:
    - regexp_replace: 패턴과 일치하는 부분을 교체
    - rlike: 패턴 매치 여부 boolean 반환
    """
    return df.withColumn(
        "zip_code",
        regexp_extract(col("address_raw"), r"(\d{5})", 1),
    )
