"""
M05: 조인과 집계
도메인: 이커머스 다중 테이블
데이터: data/ecommerce/ CSV 3종
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def revenue_by_customer(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    [Easy] inner join + groupBy로 고객별 총 매출 집계

    orders와 customers를 customer_id로 inner join한 후,
    고객별 총 주문금액(total_revenue)과 주문 건수(order_count)를 집계합니다.

    Args:
        orders_df: order_id, customer_id, amount 등 포함
        customers_df: customer_id, name, city 등 포함

    Returns:
        customer_id, name, total_revenue, order_count 컬럼을 가진 DataFrame
        total_revenue 내림차순 정렬

    Hint:
        - orders_df.join(customers_df, on="customer_id", how="inner")
        - .groupBy("customer_id", "name")
        - .agg(sum("amount").alias("total_revenue"), count("order_id").alias("order_count"))
        - from pyspark.sql.functions import sum, count
    """
    # TODO: 구현하세요
    raise NotImplementedError


def customers_without_orders(customers_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    [Easy] left outer join으로 주문 없는 고객 찾기

    모든 고객 중 한 번도 주문하지 않은 고객을 반환합니다.

    Args:
        customers_df: customer_id, name 등
        orders_df: order_id, customer_id 등

    Returns:
        주문이 없는 고객의 customer_id, name 컬럼
        (orders의 order_id가 null인 행)

    Hint:
        - customers_df.join(orders_df, on="customer_id", how="left")
        - .filter(col("order_id").isNull())
        - .select("customer_id", "name")
    """
    # TODO: 구현하세요
    raise NotImplementedError


def cross_summary(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    [Medium] 3테이블 join + 카테고리-도시 교차 집계

    orders, customers, products를 조인하여
    카테고리별, 도시별 총 매출을 집계합니다.

    Args:
        orders_df: order_id, customer_id, product_id, amount
        customers_df: customer_id, name, city
        products_df: product_id, name, category, price

    Returns:
        category, city, total_revenue 컬럼, total_revenue 내림차순 정렬

    Hint:
        - 먼저 orders와 customers를 customer_id로 join
        - 그 다음 products를 product_id로 join
        - groupBy("category", "city").agg(sum("amount").alias("total_revenue"))
    """
    # TODO: 구현하세요
    raise NotImplementedError


def category_metrics(df: DataFrame) -> DataFrame:
    """
    [Medium] 카테고리별 다중 집계

    카테고리별로 다음을 집계합니다:
    - total_revenue: 총 매출 (amount 합산)
    - avg_amount: 평균 주문금액
    - order_count: 주문 건수
    - unique_customers: 고유 고객 수

    Args:
        df: order_id, customer_id, category, amount 포함 DataFrame

    Returns:
        category, total_revenue, avg_amount, order_count, unique_customers
        total_revenue 내림차순 정렬

    Hint:
        - .groupBy("category")
        - .agg(
              sum("amount").alias("total_revenue"),
              avg("amount").alias("avg_amount"),
              count("order_id").alias("order_count"),
              countDistinct("customer_id").alias("unique_customers")
          )
        - from pyspark.sql.functions import sum, avg, count, countDistinct
    """
    # TODO: 구현하세요
    raise NotImplementedError


def order_intervals(orders_df: DataFrame) -> DataFrame:
    """
    [Hard] self-join으로 고객별 연속 주문 간격 계산

    같은 고객의 연속된 두 주문 사이의 날짜 간격을 계산합니다.

    Args:
        orders_df: order_id, customer_id, order_date(string "YYYY-MM-DD") 포함

    Returns:
        customer_id, order_id, order_date, prev_order_date, days_since_last_order
        컬럼을 가진 DataFrame (days_since_last_order 오름차순 정렬)
        첫 주문(이전 주문 없음)은 결과에서 제외

    Hint:
        - 이 문제는 Window 함수로도 풀 수 있지만, self-join으로도 풀 수 있습니다.
        - 추천: Window 함수 방법
            from pyspark.sql.functions import lag, to_date, datediff
            from pyspark.sql.window import Window
            w = Window.partitionBy("customer_id").orderBy("order_date")
            df.withColumn("prev_order_date", lag("order_date").over(w))
              .withColumn("days_since_last_order", datediff(...))
              .filter(col("prev_order_date").isNotNull())
    """
    # TODO: 구현하세요
    raise NotImplementedError
