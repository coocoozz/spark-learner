"""
M04: DataFrame 연산
도메인: 이커머스 주문
데이터: data/ecommerce/orders.csv + Faker
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def add_total_column(df: DataFrame) -> DataFrame:
    """
    [Easy] withColumn으로 total 컬럼 추가

    amount * quantity를 계산한 total 컬럼을 추가합니다.

    Args:
        df: order_id, amount(double), quantity(int) 등을 포함하는 DataFrame

    Returns:
        total = amount * quantity 컬럼이 추가된 DataFrame

    Hint:
        - from pyspark.sql.functions import col
        - df.withColumn("total", col("amount") * col("quantity"))
    """
    # TODO: 구현하세요
    raise NotImplementedError


def filter_and_sort(df: DataFrame, min_amount: float, sort_col: str) -> DataFrame:
    """
    [Easy] filter + 정렬

    amount가 min_amount 이상인 주문만 남기고,
    sort_col 기준 오름차순으로 정렬합니다.

    Args:
        df: 주문 DataFrame
        min_amount: 최소 금액 (이 값 이상)
        sort_col: 정렬 기준 컬럼명

    Returns:
        필터링 + 정렬된 DataFrame

    Hint:
        - df.filter(col("amount") >= min_amount)
        - .orderBy(sort_col) 또는 .orderBy(col(sort_col))
    """
    # TODO: 구현하세요
    raise NotImplementedError


def categorize_orders(df: DataFrame) -> DataFrame:
    """
    [Medium] when/otherwise 다중 조건 분류

    amount 기준으로 주문을 3단계로 분류한 order_tier 컬럼을 추가합니다:
    - amount >= 500: "Premium"
    - amount >= 100: "Standard"
    - 나머지: "Basic"

    Args:
        df: amount 컬럼을 포함하는 주문 DataFrame

    Returns:
        order_tier 컬럼이 추가된 DataFrame

    Hint:
        - from pyspark.sql.functions import when
        - when(condition1, value1).when(condition2, value2).otherwise(value3)
        - df.withColumn("order_tier", when(...).when(...).otherwise(...))
    """
    # TODO: 구현하세요
    raise NotImplementedError


def calculate_delivery_days(df: DataFrame) -> DataFrame:
    """
    [Medium] 날짜 파싱 + 배송기간 계산

    order_date와 delivery_date 사이의 일수를 계산한
    delivery_days 컬럼을 추가합니다.

    Args:
        df: order_date(string), delivery_date(string) 컬럼을 포함하는 DataFrame
            날짜 형식: "YYYY-MM-DD"

    Returns:
        delivery_days(int) 컬럼이 추가된 DataFrame

    Hint:
        - from pyspark.sql.functions import to_date, datediff
        - to_date(col("delivery_date"), "yyyy-MM-dd")
        - datediff(end_date, start_date): 두 날짜 간 일수 차이
        - df.withColumn("delivery_days", datediff(..., ...))
    """
    # TODO: 구현하세요
    raise NotImplementedError


def clean_address_column(df: DataFrame) -> DataFrame:
    """
    [Hard] regexp_extract로 비정형 컬럼 정제

    address_raw 컬럼에서 우편번호(5자리 숫자)를 추출해
    zip_code 컬럼을 추가합니다.

    address_raw 형식 예: "123 Main Street 12345" 또는 "서울시 강남구 06100"

    Args:
        df: address_raw 컬럼을 포함하는 DataFrame

    Returns:
        zip_code 컬럼이 추가된 DataFrame (우편번호 없으면 빈 문자열)

    Hint:
        - from pyspark.sql.functions import regexp_extract
        - regexp_extract(col("address_raw"), r"(\\d{5})", 1)
        - 첫 번째 인자: 컬럼, 두 번째: 패턴, 세 번째: 그룹 번호
    """
    # TODO: 구현하세요
    raise NotImplementedError
