"""
M08: Spark SQL
도메인: 이커머스 통합
데이터: data/ecommerce/ CSV → temp view
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def query_top_orders(spark: SparkSession, df: DataFrame, n: int) -> DataFrame:
    """
    [Easy] temp view 생성 후 SQL로 상위 N 주문 조회

    DataFrame을 임시 뷰로 등록하고, SQL로 amount 기준 상위 n개를 반환합니다.

    Args:
        spark: SparkSession
        df: order_id, customer_id, amount, category 포함 DataFrame
        n: 반환할 상위 주문 수

    Returns:
        amount 내림차순 상위 n개 주문 DataFrame

    Hint:
        - df.createOrReplaceTempView("orders")
        - spark.sql(f"SELECT * FROM orders ORDER BY amount DESC LIMIT {n}")
    """
    raise NotImplementedError


def high_value_categories(spark: SparkSession, df: DataFrame, threshold: float) -> DataFrame:
    """
    [Easy] SQL GROUP BY + HAVING으로 고매출 카테고리 조회

    카테고리별 총 매출이 threshold 이상인 카테고리를 반환합니다.

    Args:
        spark: SparkSession
        df: category, amount 포함 DataFrame
        threshold: 최소 총 매출 기준

    Returns:
        category, total_revenue 컬럼, total_revenue 내림차순 정렬

    Hint:
        - df.createOrReplaceTempView("orders")
        - SELECT category, SUM(amount) AS total_revenue
          FROM orders
          GROUP BY category
          HAVING total_revenue >= {threshold}
          ORDER BY total_revenue DESC
    """
    raise NotImplementedError


def above_average_customers(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    [Medium] 서브쿼리: 평균 주문금액 이상인 고객 조회

    전체 평균 주문금액보다 평균이 높은 고객 정보를 반환합니다.

    Args:
        spark: SparkSession
        orders_df: order_id, customer_id, amount
        customers_df: customer_id, name, city

    Returns:
        customer_id, name, city, avg_amount 컬럼

    Hint:
        - orders를 "orders_view", customers를 "customers_view"로 등록
        - 서브쿼리:
          SELECT c.customer_id, c.name, c.city, AVG(o.amount) AS avg_amount
          FROM orders_view o JOIN customers_view c ON o.customer_id = c.customer_id
          GROUP BY c.customer_id, c.name, c.city
          HAVING AVG(o.amount) > (SELECT AVG(amount) FROM orders_view)
    """
    raise NotImplementedError


def customer_segments_sql(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    [Medium] CASE WHEN + JOIN으로 고객 세그먼트 분류

    고객별 총 주문금액으로 세그먼트를 분류합니다:
    - total >= 2000: "VIP"
    - total >= 500: "Regular"
    - 나머지: "Occasional"

    Args:
        spark: SparkSession
        orders_df: customer_id, amount
        customers_df: customer_id, name

    Returns:
        customer_id, name, total_spent, segment 컬럼

    Hint:
        - CASE WHEN total_spent >= 2000 THEN 'VIP' ...
        - JOIN으로 고객 이름 연결
    """
    raise NotImplementedError


def monthly_growth_report(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """
    [Hard] CTE + 윈도우 함수 결합 월별 성장 리포트

    월별 매출과 전월 대비 성장률을 계산합니다.

    Args:
        spark: SparkSession
        orders_df: order_date(YYYY-MM-DD), amount 포함

    Returns:
        month, monthly_revenue, prev_revenue, growth_rate(%) 컬럼
        month 오름차순 정렬

    Hint:
        - DATE_FORMAT(order_date, 'yyyy-MM') AS month
        - WITH monthly AS (SELECT ... GROUP BY month),
          ranked AS (SELECT *, LAG(monthly_revenue) OVER (ORDER BY month) AS prev_revenue FROM monthly)
          SELECT *, (monthly_revenue - prev_revenue) / prev_revenue * 100 AS growth_rate FROM ranked
    """
    raise NotImplementedError
