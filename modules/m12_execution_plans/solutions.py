"""
M12: 실행 계획 분석 - 솔루션
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col


def identify_plan_nodes(df: DataFrame) -> dict:
    """
    풀이 설명:

    실행 계획(Execution Plan) 단계:
    1. Parsed Logical Plan: SQL/API를 파싱한 결과
    2. Analyzed Logical Plan: 이름 해석, 타입 체크
    3. Optimized Logical Plan: Catalyst 규칙 최적화
    4. Physical Plan: 실제 실행 전략

    explain() 모드:
    - explain("simple"): 물리 계획만
    - explain("extended"): 논리 + 물리 계획
    - explain("codegen"): 코드 생성 정보
    - explain("cost"): 비용 기반 최적화 정보
    - explain("formatted"): 가독성 높은 형식

    주요 노드:
    - Filter: 조건 필터링
    - Project: 컬럼 선택/변환
    - Exchange: 셔플 (Wide Transformation)
    - HashAggregate: 집계
    - SortMergeJoin / BroadcastHashJoin: 조인
    """
    plan = df._jdf.queryExecution().simpleString()
    return {
        "has_filter": "Filter" in plan,
        "has_project": "Project" in plan,
        "has_exchange": "Exchange" in plan,
        "plan_text": plan,
    }


def compare_pushdown(spark: SparkSession, path: str, filter_col: str, value: str) -> dict:
    """
    풀이 설명:

    Predicate Pushdown:
    - 필터를 데이터 소스(파일 읽기) 단계로 밀어내는 최적화
    - Parquet: 컬럼형 저장, 통계 정보(min/max)로 불필요한 row group 스킵
    - 읽는 데이터 양 감소 → I/O 비용 절감

    explain()에서 확인:
    - "PushedFilters": 소스로 푸시된 필터 목록
    - "ReadSchema": 읽는 컬럼 목록 (컬럼 pruning)

    Catalyst의 주요 최적화 규칙:
    - Predicate Pushdown: 필터를 소스 가까이 이동
    - Column Pruning: 불필요한 컬럼 제거
    - Constant Folding: 상수 표현식 미리 계산
    - Join Reordering: 조인 순서 최적화
    """
    df = spark.read.parquet(path)
    filtered_df = df.filter(col(filter_col) == value)
    plan = filtered_df._jdf.queryExecution().simpleString()
    return {
        "pushdown_plan": plan,
        "has_pushed_filters": "PushedFilters" in plan or "pushed" in plan.lower(),
    }


def identify_join_strategy(df: DataFrame) -> str:
    """
    풀이 설명:

    조인 전략 종류:
    1. BroadcastHashJoin: 작은 테이블을 broadcast, 셔플 없음 (가장 빠름)
    2. SortMergeJoin: 양쪽 정렬 후 병합, 기본 전략 (대용량에 안정적)
    3. ShuffledHashJoin: 한쪽 셔플 후 해시 조인
    4. BroadcastNestedLoopJoin: 브로드캐스트 + 중첩 루프 (비효율, 등가 조인 아닐 때)
    5. CartesianProduct: CROSS JOIN (조인 키 없음)

    전략 선택 기준:
    - 한쪽 테이블이 작으면: BroadcastHashJoin (자동 또는 hint)
    - 양쪽 모두 클 때: SortMergeJoin
    - 특수 경우: CartesianProduct, BroadcastNestedLoopJoin

    Spark UI > SQL 탭에서 시각적으로 확인 가능
    """
    plan = df._jdf.queryExecution().simpleString()
    for strategy in ["BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
                     "BroadcastNestedLoopJoin", "CartesianProduct"]:
        if strategy in plan:
            return strategy
    return "Unknown"


def optimize_query(spark: SparkSession, df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    풀이 설명:

    최적화 전략:

    1. 컬럼 Pruning (Column Pruning):
       - 필요한 컬럼만 미리 select → 조인/셔플 데이터 크기 감소
       - df1.select("order_id", "customer_id", "amount", "category")

    2. Broadcast Join:
       - df2, df3 같은 작은 테이블은 broadcast 힌트 적용
       - 셔플 없이 각 executor에서 로컬 조인

    3. 필터 밀기 (Filter Pushdown):
       - 조인 전에 불필요한 행을 미리 제거
       - Catalyst가 자동 적용하지만 명시적으로도 가능

    4. 조인 순서:
       - 가장 큰 테이블(df1)을 기준으로 작은 테이블들을 broadcast join
       - 중간 결과 크기 최소화

    비교: 비효율적인 방법
    - cross join 후 filter: 전체 곱집합 생성 후 필터링
    - 불필요한 컬럼 포함 조인: 불필요한 데이터 셔플
    """
    # 필요한 컬럼만 선택
    df1_slim = df1.select("order_id", "customer_id", "amount", "category")
    df2_slim = df2.select("customer_id", "name", "city")
    df3_slim = df3.select("category", "description")

    return (
        df1_slim
        .join(broadcast(df2_slim), on="customer_id", how="inner")
        .join(broadcast(df3_slim), on="category", how="inner")
    )
