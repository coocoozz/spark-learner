"""
M02: RDD 심화 (Pair RDD)
도메인: 이커머스 주문
데이터: src/helpers/data_generator.py로 생성
"""
from __future__ import annotations

from pyspark import RDD


def revenue_by_category(rdd: RDD) -> list[tuple[str, float]]:
    """
    [Easy] 카테고리별 총 매출 계산 (reduceByKey)

    주문 RDD에서 카테고리별 총 매출 금액을 계산합니다.

    Args:
        rdd: (category, amount) 형태의 Pair RDD
             예: [("Electronics", 1200.0), ("Clothing", 85.5), ...]

    Returns:
        [(category, total_revenue), ...] 리스트, 카테고리 오름차순 정렬
        예: [("Books", 420.0), ("Clothing", 1260.5), ("Electronics", 8500.0), ...]

    Hint:
        - reduceByKey(lambda a, b: a + b): 같은 카테고리의 금액을 합산
        - sortBy(lambda x: x[0]): 카테고리명 알파벳 순 정렬
        - collect(): 결과를 Python 리스트로 변환
    """
    # TODO: 구현하세요
    raise NotImplementedError


def orders_per_user(rdd: RDD) -> dict[str, int]:
    """
    [Easy] 유저별 주문 건수 계산 (countByKey)

    Args:
        rdd: (customer_id, order_id) 형태의 Pair RDD
             예: [("C001", "O001"), ("C002", "O002"), ("C001", "O004"), ...]

    Returns:
        {customer_id: order_count} 딕셔너리
        예: {"C001": 5, "C002": 3, ...}

    Hint:
        - countByKey(): 각 키의 등장 횟수를 딕셔너리로 반환 (Action)
        - defaultdict나 dict()로 변환하여 반환
    """
    # TODO: 구현하세요
    raise NotImplementedError


def top_products_per_category(rdd: RDD, n: int) -> list[tuple[str, list]]:
    """
    [Medium] 카테고리별 매출 상위 N 상품 (groupByKey)

    Args:
        rdd: (category, (product_id, amount)) 형태의 Pair RDD

    Returns:
        [(category, [(product_id, amount), ...]), ...] 리스트
        각 카테고리 내에서 amount 내림차순으로 상위 n개

    Hint:
        - groupByKey(): 같은 키의 값들을 iterable로 그룹화
        - sorted(values, key=lambda x: x[1], reverse=True)[:n]
        - mapValues(): 값에만 함수 적용
    """
    # TODO: 구현하세요
    raise NotImplementedError


def net_revenue(orders_rdd: RDD, returns_rdd: RDD) -> list[tuple[str, float]]:
    """
    [Medium] 주문-반품 join으로 카테고리별 순매출 계산

    주문 RDD와 반품 RDD를 카테고리별로 join하여
    (총 주문금액 - 총 반품금액)을 계산합니다.

    Args:
        orders_rdd: (category, total_amount) 형태의 Pair RDD
        returns_rdd: (category, refund_amount) 형태의 Pair RDD

    Returns:
        [(category, net_revenue), ...] 리스트, 카테고리 오름차순
        순매출 = 총 주문금액 - 총 반품금액

    Hint:
        - 먼저 각 RDD를 reduceByKey로 카테고리별 합산
        - leftOuterJoin(): 반품이 없는 카테고리도 포함
        - mapValues()로 net_revenue 계산 (refund가 None이면 0으로 처리)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def category_diversity(rdd: RDD, total_categories: int) -> list[tuple[str, float]]:
    """
    [Hard] 구매 카테고리 다양성 지수 계산 (combineByKey)

    각 고객이 구매한 카테고리의 다양성을 수치화합니다.
    다양성 지수 = 고객이 구매한 고유 카테고리 수 / 전체 카테고리 수

    Args:
        rdd: (customer_id, category) 형태의 Pair RDD
        total_categories: 전체 카테고리 수 (예: 5)

    Returns:
        [(customer_id, diversity_index), ...] 리스트, 지수 내림차순 정렬
        다양성 지수는 0.0 ~ 1.0 사이 값

    Hint:
        combineByKey를 사용하여 고객별 고유 카테고리 집합을 만들어야 합니다:
        - createCombiner: lambda cat: {cat}  (첫 값으로 set 생성)
        - mergeValue: lambda acc, cat: acc | {cat}  (새 카테고리 추가)
        - mergeCombiners: lambda a, b: a | b  (두 set 합치기)
        최종적으로 집합 크기 / total_categories = 다양성 지수
    """
    # TODO: 구현하세요
    raise NotImplementedError
