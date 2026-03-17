"""
M08: Spark SQL - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def query_top_orders(spark: SparkSession, df: DataFrame, n: int) -> DataFrame:
    """
    풀이 설명:

    createOrReplaceTempView vs createTempView:
    - createOrReplaceTempView: 이미 존재하면 덮어씀 (권장)
    - createTempView: 이미 존재하면 TempTableAlreadyExistsException

    Global Temp View vs Session Temp View:
    - createOrReplaceTempView: 현재 SparkSession에서만 접근
    - createOrReplaceGlobalTempView: 다른 SparkSession에서도 접근 가능
      → 접근 시 "global_temp.view_name" 형식 사용

    SQL은 DataFrame API와 동일한 Catalyst 최적화 적용됩니다.
    """
    df.createOrReplaceTempView("orders_m08_top")
    return spark.sql(f"SELECT * FROM orders_m08_top ORDER BY amount DESC LIMIT {n}")


def high_value_categories(spark: SparkSession, df: DataFrame, threshold: float) -> DataFrame:
    """
    풀이 설명:

    HAVING vs WHERE:
    - WHERE: 그룹화 전 행 필터링
    - HAVING: 그룹화 후 집계 결과 필터링

    GROUP BY + HAVING 패턴은 집계 조건 필터에 필수입니다.

    Spark SQL은 ANSI SQL을 대부분 지원합니다.
    """
    df.createOrReplaceTempView("orders_m08_cat")
    return spark.sql(f"""
        SELECT category, SUM(amount) AS total_revenue
        FROM orders_m08_cat
        GROUP BY category
        HAVING SUM(amount) >= {threshold}
        ORDER BY total_revenue DESC
    """)


def above_average_customers(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    상관 서브쿼리(Correlated Subquery):
    HAVING AVG(o.amount) > (SELECT AVG(amount) FROM orders_view)
    - 내부 서브쿼리가 외부 쿼리와 독립적으로 실행됩니다.
    - Spark는 이를 자동으로 최적화합니다.

    DataFrame API 대안:
    avg_amount = orders_df.agg(avg("amount")).first()[0]
    df.filter(col("avg_amount") > avg_amount)
    """
    orders_df.createOrReplaceTempView("orders_m08_cust")
    customers_df.createOrReplaceTempView("customers_m08")
    return spark.sql("""
        SELECT c.customer_id, c.name, c.city, AVG(o.amount) AS avg_amount
        FROM orders_m08_cust o
        JOIN customers_m08 c ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.city
        HAVING AVG(o.amount) > (SELECT AVG(amount) FROM orders_m08_cust)
        ORDER BY avg_amount DESC
    """)


def customer_segments_sql(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    CTE(Common Table Expression)를 사용한 가독성 향상:
    WITH totals AS (
        SELECT customer_id, SUM(amount) AS total_spent
        FROM orders GROUP BY customer_id
    )
    SELECT ... FROM totals JOIN customers ...

    CASE WHEN은 SQL과 DataFrame API(when/otherwise) 모두 지원합니다.
    SQL로 작성하면 복잡한 조건도 가독성 있게 표현 가능합니다.
    """
    orders_df.createOrReplaceTempView("orders_m08_seg")
    customers_df.createOrReplaceTempView("customers_m08_seg")
    return spark.sql("""
        WITH totals AS (
            SELECT customer_id, SUM(amount) AS total_spent
            FROM orders_m08_seg
            GROUP BY customer_id
        )
        SELECT
            t.customer_id,
            c.name,
            t.total_spent,
            CASE
                WHEN t.total_spent >= 2000 THEN 'VIP'
                WHEN t.total_spent >= 500  THEN 'Regular'
                ELSE 'Occasional'
            END AS segment
        FROM totals t
        JOIN customers_m08_seg c ON t.customer_id = c.customer_id
        ORDER BY t.total_spent DESC
    """)


def monthly_growth_report(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    CTE(Common Table Expression) 체이닝:
    WITH a AS (...), b AS (...) SELECT ... FROM b

    LAG() OVER (ORDER BY month):
    - 파티션 없이 전체에 대해 ORDER BY만 지정
    - 이전 월의 매출 가져오기

    DATE_FORMAT vs date_trunc:
    - DATE_FORMAT(date, 'yyyy-MM'): 월 문자열 형식
    - date_trunc('month', date): 월의 첫 날로 잘라냄

    성장률 = (이번달 - 지난달) / 지난달 * 100
    첫 번째 달은 prev_revenue가 NULL → growth_rate도 NULL
    """
    orders_df.createOrReplaceTempView("orders_m08_growth")
    return spark.sql("""
        WITH monthly AS (
            SELECT
                DATE_FORMAT(order_date, 'yyyy-MM') AS month,
                SUM(amount) AS monthly_revenue
            FROM orders_m08_growth
            GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
        ),
        ranked AS (
            SELECT
                month,
                monthly_revenue,
                LAG(monthly_revenue) OVER (ORDER BY month) AS prev_revenue
            FROM monthly
        )
        SELECT
            month,
            monthly_revenue,
            prev_revenue,
            (monthly_revenue - prev_revenue) / prev_revenue * 100 AS growth_rate
        FROM ranked
        ORDER BY month
    """)
