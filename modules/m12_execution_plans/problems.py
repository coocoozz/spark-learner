"""
M12: 실행 계획 분석
도메인: 통합
데이터: 기존 재활용
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def identify_plan_nodes(df: DataFrame) -> dict:
    """
    [Easy] explain() 파싱 - 노드 식별

    DataFrame의 실행 계획(explain)을 분석하여
    주요 노드 유무를 확인합니다.

    Args:
        df: 분석할 DataFrame

    Returns:
        {
            "has_filter": bool,       # Filter 노드 포함 여부
            "has_project": bool,      # Project 노드 포함 여부
            "has_exchange": bool,     # Exchange (셔플) 노드 포함 여부
            "plan_text": str,         # explain() 결과 전체 텍스트
        }

    Hint:
        - import io
        - df.explain(mode="simple") 또는 df.explain()
        - explain 결과를 문자열로 캡처하려면:
          from pyspark.sql import SparkSession
          # df._jdf.queryExecution().simpleString() 으로 얻을 수 있음
          # 또는 explain(extended=False)를 print 대신 string으로 얻는 방법
        - plan = df._jdf.queryExecution().simpleString()
        - "Filter" in plan, "Project" in plan, "Exchange" in plan
    """
    raise NotImplementedError


def compare_pushdown(spark: SparkSession, path: str, filter_col: str, value: str) -> dict:
    """
    [Medium] Predicate Pushdown 전후 비교

    Parquet 파일에서 필터를 적용할 때:
    - 필터를 파일 읽기 전에 푸시다운하면 읽는 데이터가 줄어듦
    - explain()에서 "PushedFilters" 항목으로 확인

    Args:
        spark: SparkSession
        path: Parquet 파일 경로
        filter_col: 필터 적용 컬럼명
        value: 필터 값

    Returns:
        {
            "pushdown_plan": str,       # 필터 포함 읽기 plan
            "has_pushed_filters": bool, # PushedFilters 포함 여부
        }

    Hint:
        - df = spark.read.parquet(path)
        - filtered_df = df.filter(col(filter_col) == value)
        - plan = filtered_df._jdf.queryExecution().simpleString()
        - "PushedFilters" in plan 또는 "pushed" in plan.lower()
    """
    raise NotImplementedError


def identify_join_strategy(df: DataFrame) -> str:
    """
    [Medium] 조인 전략 식별

    explain()에서 사용된 조인 전략을 식별합니다.

    Args:
        df: join이 포함된 DataFrame

    Returns:
        조인 전략 문자열:
        "BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
        "BroadcastNestedLoopJoin", "CartesianProduct", "Unknown" 중 하나

    Hint:
        - plan = df._jdf.queryExecution().simpleString()
        - 다음 문자열들 검색:
          "BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin"
    """
    raise NotImplementedError


def optimize_query(spark: SparkSession, df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    [Hard] 비효율 쿼리 최적화

    다음 비효율적인 패턴을 최적화하여 구현합니다:
    1. df1과 df2의 cross join 후 filter → join으로 변경
    2. df3 집계 후 join → broadcast join 적용
    3. 불필요한 컬럼을 미리 select로 제거

    Args:
        spark: SparkSession
        df1: order_id, customer_id, amount, category 포함
        df2: customer_id, name, city (작은 테이블)
        df3: category, description (매우 작은 테이블)

    Returns:
        최적화된 조인 결과 DataFrame

    Hint:
        - df1.join(df2, on="customer_id") 로 조인 (cross join 대신)
        - broadcast(df2): 작은 테이블 broadcast
        - broadcast(df3): 더 작은 테이블 broadcast
        - 먼저 필요한 컬럼만 select하여 조인 크기 축소
    """
    raise NotImplementedError
