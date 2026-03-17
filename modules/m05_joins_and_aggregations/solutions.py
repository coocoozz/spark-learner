"""
M05: 조인과 집계 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, countDistinct, datediff, lag, sum, to_date
from pyspark.sql.window import Window


def revenue_by_customer(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    1. inner join: 양쪽에 모두 있는 customer_id만 포함
       - on="customer_id": 동일 이름 컬럼으로 조인 (자동 중복 제거)
       - on=orders_df["customer_id"] == customers_df["customer_id"]: 명시적 조인
         (이 경우 두 컬럼이 모두 남으므로 주의)

    2. groupBy + agg: 여러 집계를 한 번에 수행
       - sum, count는 집계 함수 (from pyspark.sql.functions)
       - Python 내장 sum()과 충돌 주의 → from pyspark.sql import functions as F 패턴 추천

    3. orderBy("total_revenue", ascending=False): 내림차순
    """
    return (
        orders_df
        .join(customers_df, on="customer_id", how="inner")
        .groupBy("customer_id", "name")
        .agg(
            sum("amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
        )
        .orderBy("total_revenue", ascending=False)
    )


def customers_without_orders(customers_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    Left outer join 패턴으로 "없는 것" 찾기:
    1. customers LEFT JOIN orders ON customer_id
    2. 오른쪽(orders)의 컬럼이 null인 행 = 주문 없는 고객

    SQL로 표현하면:
    SELECT c.customer_id, c.name
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_id IS NULL

    isNull() vs == None:
    - col("order_id").isNull(): Spark 컬럼의 null 체크
    - col("order_id") == None: 파이썬 None과 비교 (권장하지 않음)
    """
    return (
        customers_df
        .join(orders_df.select("customer_id", "order_id"), on="customer_id", how="left")
        .filter(col("order_id").isNull())
        .select("customer_id", "name")
    )


def cross_summary(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    3개 테이블 조인 전략:
    1. orders + customers → 주문자 정보 추가
    2. (1의 결과) + products → 상품 카테고리 정보 추가
    3. groupBy(category, city).agg(sum)

    컬럼 모호성 처리:
    - 동일한 이름의 컬럼이 여러 DataFrame에 있을 수 있습니다.
    - on="customer_id" 방식은 조인 후 컬럼이 하나만 남습니다.
    - customers_df에 "name"이 있고, products_df에도 "name"이 있을 수 있어
      select에서 명시적으로 지정합니다.
    """
    orders_with_city = orders_df.join(
        customers_df.select("customer_id", "city"), on="customer_id", how="inner"
    )
    orders_with_cat = orders_with_city.join(
        products_df.select("product_id", "category"), on="product_id", how="inner"
    )
    return (
        orders_with_cat
        .groupBy("category", "city")
        .agg(sum("amount").alias("total_revenue"))
        .orderBy("total_revenue", ascending=False)
    )


def category_metrics(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    agg() 내에서 여러 집계 함수를 한 번에 사용합니다.
    각 집계 함수는 다른 컬럼에 적용할 수 있습니다.

    countDistinct("customer_id"): 고유 고객 수
    - COUNT(DISTINCT customer_id) 에 해당
    - approx_count_distinct(): 근사값이지만 훨씬 빠름 (대규모 데이터에 권장)

    avg() = mean(): 평균 계산
    round(avg("amount"), 2): 소수점 2자리로 반올림
    """
    return (
        df
        .groupBy("category")
        .agg(
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_amount"),
            count("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("total_revenue", ascending=False)
    )


def order_intervals(orders_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    Window 함수를 사용한 풀이:

    1. Window 정의:
       - partitionBy("customer_id"): 고객별로 독립적인 윈도우
       - orderBy("order_date"): 날짜 순으로 정렬

    2. lag("order_date", 1): 이전 행의 order_date 가져오기
       - lag는 현재 행 기준으로 n행 앞의 값을 반환
       - 첫 번째 행에는 이전 행이 없으므로 null 반환

    3. datediff로 날짜 차이 계산

    4. filter(isNotNull): 첫 주문(이전 주문 없음) 제외

    Window 함수 개념:
    - 파티션 내에서 현재 행과 주변 행의 관계를 계산
    - groupBy처럼 행을 줄이지 않고 새 컬럼을 추가
    """
    w = Window.partitionBy("customer_id").orderBy("order_date")
    return (
        orders_df
        .withColumn("prev_order_date", lag("order_date", 1).over(w))
        .withColumn(
            "days_since_last_order",
            datediff(
                to_date(col("order_date"), "yyyy-MM-dd"),
                to_date(col("prev_order_date"), "yyyy-MM-dd"),
            ),
        )
        .filter(col("prev_order_date").isNotNull())
        .select("customer_id", "order_id", "order_date", "prev_order_date", "days_since_last_order")
        .orderBy("days_since_last_order")
    )
