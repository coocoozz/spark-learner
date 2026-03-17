"""
M06: 윈도우 함수
도메인: 금융 주가
데이터: data/finance/stock_prices.csv
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def rank_by_price(df: DataFrame) -> DataFrame:
    """
    [Easy] 종목별 종가 순위 (rank)

    각 날짜에서 종가(close) 기준으로 종목 순위를 매깁니다.
    같은 날짜 내에서 close 내림차순으로 순위를 부여합니다.

    Args:
        df: symbol, date, close 등을 포함하는 주가 DataFrame

    Returns:
        symbol, date, close, price_rank 컬럼 포함 DataFrame

    Hint:
        - from pyspark.sql.window import Window
        - from pyspark.sql.functions import rank
        - w = Window.partitionBy("date").orderBy(col("close").desc())
        - df.withColumn("price_rank", rank().over(w))
    """
    raise NotImplementedError


def cumulative_volume(df: DataFrame) -> DataFrame:
    """
    [Easy] 종목별 날짜순 누적 거래량

    각 종목(symbol)에 대해 날짜 순서대로 누적 거래량을 계산합니다.

    Args:
        df: symbol, date, volume 컬럼 포함

    Returns:
        symbol, date, volume, cumulative_volume 컬럼 포함 DataFrame

    Hint:
        - w = Window.partitionBy("symbol").orderBy("date")
        - from pyspark.sql.functions import sum
        - sum("volume").over(w): 처음부터 현재 행까지 누적합산
    """
    raise NotImplementedError


def moving_averages(df: DataFrame, windows: list[int]) -> DataFrame:
    """
    [Medium] 종목별 이동평균 계산

    각 종목에 대해 지정된 윈도우 크기의 이동평균을 계산합니다.
    예: windows=[5, 20]이면 ma_5, ma_20 컬럼 추가

    Args:
        df: symbol, date, close 포함 DataFrame
        windows: 이동평균 윈도우 크기 리스트 (예: [5, 20])

    Returns:
        ma_{n} 컬럼들이 추가된 DataFrame

    Hint:
        - base_w = Window.partitionBy("symbol").orderBy("date")
        - rowsBetween(-(n-1), 0): 현재 포함 n개 행
        - avg("close").over(w.rowsBetween(-(n-1), 0))
        - windows 리스트를 순회하면서 withColumn으로 추가
    """
    raise NotImplementedError


def daily_returns_and_streaks(df: DataFrame) -> DataFrame:
    """
    [Medium] 전일 대비 수익률 계산

    각 종목의 전일 종가 대비 수익률과 상승 여부를 계산합니다.

    Args:
        df: symbol, date, close 포함 DataFrame

    Returns:
        symbol, date, close, prev_close, daily_return(%), is_up(boolean) 컬럼

    Hint:
        - w = Window.partitionBy("symbol").orderBy("date")
        - lag("close", 1).over(w): 전일 종가
        - daily_return = (close - prev_close) / prev_close * 100
        - is_up = close > prev_close
    """
    raise NotImplementedError


def max_drawdown(df: DataFrame) -> DataFrame:
    """
    [Hard] 종목별 최대 낙폭(Max Drawdown) 계산

    drawdown = (running_max - close) / running_max * 100

    Args:
        df: symbol, date, close 포함 DataFrame

    Returns:
        symbol, date, close, running_max, drawdown(%) 컬럼 포함 DataFrame
        drawdown 내림차순 정렬

    Hint:
        - w = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        - max("close").over(w): 누적 최고가
    """
    raise NotImplementedError
