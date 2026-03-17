"""
M06: 윈도우 함수 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lag, max, rank, sum
from pyspark.sql.window import Window


def rank_by_price(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    Window 함수 구성 요소:
    1. PARTITION BY: partitionBy("date") → 날짜별 독립 그룹
    2. ORDER BY: orderBy(col("close").desc()) → 종가 내림차순

    rank() vs dense_rank() vs row_number():
    - rank(): 동점 시 같은 순위, 다음 순위 건너뜀 (1,1,3)
    - dense_rank(): 동점 시 같은 순위, 다음 순위 연속 (1,1,2)
    - row_number(): 고유한 순위 (1,2,3)
    """
    w = Window.partitionBy("date").orderBy(col("close").desc())
    return df.withColumn("price_rank", rank().over(w))


def cumulative_volume(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    orderBy를 포함한 Window의 기본 frame:
    - rangeBetween(unboundedPreceding, currentRow) = 처음부터 현재 행까지

    sum("volume").over(w): 누적합 (Cumulative Sum / Running Total)
    """
    w = Window.partitionBy("symbol").orderBy("date")
    return df.withColumn("cumulative_volume", sum("volume").over(w))


def moving_averages(df: DataFrame, windows: list[int]) -> DataFrame:
    """
    풀이 설명:

    rowsBetween(-(n-1), 0):
    - n=5이면 rowsBetween(-4, 0) → 현재 포함 5개 행의 평균

    데이터가 n개 미만인 경우 사용 가능한 행만으로 평균 계산
    """
    base_w = Window.partitionBy("symbol").orderBy("date")
    result = df
    for n in windows:
        w = base_w.rowsBetween(-(n - 1), 0)
        result = result.withColumn(f"ma_{n}", avg("close").over(w))
    return result


def daily_returns_and_streaks(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    lag(col, n): 현재 행보다 n행 앞의 값
    - 첫 번째 행은 null 반환

    daily_return = (오늘 - 어제) / 어제 * 100
    is_up: BooleanType 컬럼
    """
    w = Window.partitionBy("symbol").orderBy("date")
    return (
        df
        .withColumn("prev_close", lag("close", 1).over(w))
        .withColumn(
            "daily_return",
            ((col("close") - col("prev_close")) / col("prev_close") * 100),
        )
        .withColumn("is_up", col("close") > col("prev_close"))
    )


def max_drawdown(df: DataFrame) -> DataFrame:
    """
    풀이 설명:

    running_max: 처음부터 현재까지의 최고가
    drawdown = (고점 - 현재가) / 고점 * 100

    Window.unboundedPreceding: 파티션의 첫 번째 행부터
    """
    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    return (
        df
        .withColumn("running_max", max("close").over(w))
        .withColumn(
            "drawdown",
            (col("running_max") - col("close")) / col("running_max") * 100,
        )
        .orderBy("drawdown", ascending=False)
    )
