"""
M02: RDD 심화 (Pair RDD) - 솔루션
"""
from __future__ import annotations

from pyspark import RDD


def revenue_by_category(rdd: RDD) -> list[tuple[str, float]]:
    """
    풀이 설명:

    reduceByKey는 같은 키를 가진 값들을 reduce 함수로 집계합니다.
    map → (카테고리, 금액) 쌍이 이미 주어지므로 바로 reduceByKey 사용.

    reduceByKey vs groupByKey:
    - reduceByKey: 각 파티션에서 먼저 집계 후 셔플 → 효율적
    - groupByKey: 모든 값을 셔플 후 집계 → 메모리 비효율

    sortBy(lambda x: x[0]): 튜플의 첫 번째 요소(카테고리명)로 정렬
    """
    return (
        rdd
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[0])
        .collect()
    )


def orders_per_user(rdd: RDD) -> dict[str, int]:
    """
    풀이 설명:

    countByKey()는 각 키의 등장 횟수를 집계하는 Action입니다.
    내부적으로 reduceByKey(lambda a, b: a + b)와 유사하게 동작하지만
    딕셔너리를 직접 반환합니다.

    주의: countByKey()는 결과를 드라이버 메모리에 모두 모읍니다.
    키 종류가 매우 많으면 map(lambda x: (x[0], 1)).reduceByKey(+)가 낫습니다.
    """
    return dict(rdd.countByKey())


def top_products_per_category(rdd: RDD, n: int) -> list[tuple[str, list]]:
    """
    풀이 설명:

    groupByKey()는 같은 키의 모든 값을 ResultIterable로 묶습니다.
    mapValues()를 사용해 값(iterable)을 정렬 후 상위 n개로 자릅니다.

    groupByKey 주의사항:
    - 모든 값을 메모리에 올리므로 값이 많으면 OOM 위험
    - 단순 집계라면 reduceByKey가 낫지만, 상위 N 추출처럼
      전체 값이 필요한 경우에는 groupByKey가 적합합니다.

    mapValues(func): 값에만 함수를 적용 (키는 변경하지 않음)
    """
    return (
        rdd
        .groupByKey()
        .mapValues(lambda items: sorted(items, key=lambda x: x[1], reverse=True)[:n])
        .sortBy(lambda x: x[0])
        .collect()
    )


def net_revenue(orders_rdd: RDD, returns_rdd: RDD) -> list[tuple[str, float]]:
    """
    풀이 설명:

    1단계: 각 RDD를 카테고리별로 집계합니다.
       - orders: (category, total_sales)
       - returns: (category, total_refunds)

    2단계: leftOuterJoin으로 조인합니다.
       - leftOuterJoin: 왼쪽(주문)에 없는 카테고리는 제외
       - 오른쪽(반품)에 없는 카테고리는 None으로 표시

    3단계: 순매출 계산
       - (sales, refund_or_none) → sales - (refund or 0)

    join 종류:
    - join: inner join (양쪽 모두 있는 키만)
    - leftOuterJoin: 왼쪽 키는 모두 포함
    - rightOuterJoin: 오른쪽 키는 모두 포함
    - fullOuterJoin: 양쪽 키 모두 포함
    """
    agg_orders = orders_rdd.reduceByKey(lambda a, b: a + b)
    agg_returns = returns_rdd.reduceByKey(lambda a, b: a + b)

    return (
        agg_orders
        .leftOuterJoin(agg_returns)
        .mapValues(lambda x: x[0] - (x[1] or 0.0))
        .sortBy(lambda x: x[0])
        .collect()
    )


def category_diversity(rdd: RDD, total_categories: int) -> list[tuple[str, float]]:
    """
    풀이 설명:

    combineByKey는 가장 유연한 집계 API입니다. 세 가지 함수를 받습니다:

    1. createCombiner(value): 키의 첫 번째 값으로 accumulator 초기화
       → lambda cat: {cat}  # 첫 카테고리로 set 생성

    2. mergeValue(acc, value): 같은 파티션 내에서 새 값을 accumulator에 추가
       → lambda acc, cat: acc | {cat}  # set에 새 카테고리 추가

    3. mergeCombiners(acc1, acc2): 다른 파티션의 accumulator들을 합치기
       → lambda a, b: a | b  # 두 set을 합집합

    combineByKey 동작 원리:
    파티션1: C001→{Elec}, C001+Clothing→{Elec, Cloth}
    파티션2: C001→{Food}
    최종 merge: {Elec, Cloth} | {Food} = {Elec, Cloth, Food}
    다양성 지수 = 3 / 5 = 0.6

    고급 사용: aggregateByKey도 유사하게 작동하며 초기값을 명시합니다.
    """
    return (
        rdd
        .combineByKey(
            lambda cat: {cat},
            lambda acc, cat: acc | {cat},
            lambda a, b: a | b,
        )
        .mapValues(lambda categories: len(categories) / total_categories)
        .sortBy(lambda x: x[1], ascending=False)
        .collect()
    )
