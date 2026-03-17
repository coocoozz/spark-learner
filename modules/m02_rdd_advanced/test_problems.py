"""
M02: RDD 심화 (Pair RDD) - 테스트
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

from modules.m02_rdd_advanced.problems import (
    category_diversity,
    net_revenue,
    orders_per_user,
    revenue_by_category,
    top_products_per_category,
)


@pytest.fixture
def orders_rdd(sc):
    data = [
        ("Electronics", 1200.0),
        ("Clothing", 85.5),
        ("Electronics", 450.0),
        ("Books", 29.99),
        ("Clothing", 120.0),
        ("Food", 35.20),
        ("Electronics", 899.0),
        ("Books", 45.0),
        ("Food", 22.50),
    ]
    return sc.parallelize(data)


@pytest.fixture
def user_orders_rdd(sc):
    data = [
        ("C001", "O001"),
        ("C002", "O002"),
        ("C001", "O004"),
        ("C003", "O003"),
        ("C001", "O012"),
        ("C002", "O007"),
        ("C003", "O010"),
        ("C001", "O022"),
    ]
    return sc.parallelize(data)


@pytest.fixture
def category_product_rdd(sc):
    data = [
        ("Electronics", ("P010", 1200.0)),
        ("Electronics", ("P090", 899.0)),
        ("Electronics", ("P040", 450.0)),
        ("Clothing", ("P060", 120.0)),
        ("Clothing", ("P020", 85.5)),
        ("Books", ("P070", 45.0)),
        ("Books", ("P030", 29.99)),
        ("Food", ("P100", 67.8)),
        ("Food", ("P050", 35.2)),
        ("Food", ("P080", 22.5)),
    ]
    return sc.parallelize(data)


@pytest.fixture
def returns_rdd(sc):
    data = [
        ("Electronics", 300.0),
        ("Clothing", 50.0),
    ]
    return sc.parallelize(data)


@pytest.fixture
def customer_category_rdd(sc):
    data = [
        ("C001", "Electronics"),
        ("C001", "Clothing"),
        ("C001", "Electronics"),
        ("C001", "Books"),
        ("C002", "Food"),
        ("C002", "Electronics"),
        ("C003", "Books"),
        ("C003", "Books"),
    ]
    return sc.parallelize(data)


class TestEasy:
    def test_revenue_by_category_returns_list(self, orders_rdd):
        result = revenue_by_category(orders_rdd)
        assert isinstance(result, list)

    def test_revenue_by_category_correct(self, orders_rdd):
        result = revenue_by_category(orders_rdd)
        result_dict = dict(result)
        assert abs(result_dict["Electronics"] - 2549.0) < 0.01
        assert abs(result_dict["Clothing"] - 205.5) < 0.01
        assert abs(result_dict["Books"] - 74.99) < 0.01

    def test_revenue_by_category_sorted(self, orders_rdd):
        result = revenue_by_category(orders_rdd)
        categories = [c for c, _ in result]
        assert categories == sorted(categories)

    def test_orders_per_user_returns_dict(self, user_orders_rdd):
        result = orders_per_user(user_orders_rdd)
        assert isinstance(result, dict)

    def test_orders_per_user_counts(self, user_orders_rdd):
        result = orders_per_user(user_orders_rdd)
        assert result["C001"] == 4
        assert result["C002"] == 2
        assert result["C003"] == 2


class TestMedium:
    def test_top_products_per_category_returns_list(self, category_product_rdd):
        result = top_products_per_category(category_product_rdd, 2)
        assert isinstance(result, list)

    def test_top_products_per_category_count(self, category_product_rdd):
        result = top_products_per_category(category_product_rdd, 2)
        for _, products in result:
            assert len(products) <= 2

    def test_top_products_per_category_sorted_by_amount(self, category_product_rdd):
        result = top_products_per_category(category_product_rdd, 2)
        result_dict = dict(result)
        electronics = result_dict["Electronics"]
        assert electronics[0][1] >= electronics[1][1]

    def test_net_revenue_correct(self, orders_rdd, returns_rdd):
        result = net_revenue(orders_rdd, returns_rdd)
        result_dict = dict(result)
        assert abs(result_dict["Electronics"] - (2549.0 - 300.0)) < 0.01
        assert abs(result_dict["Clothing"] - (205.5 - 50.0)) < 0.01

    def test_net_revenue_includes_no_return_categories(self, orders_rdd, returns_rdd):
        result = net_revenue(orders_rdd, returns_rdd)
        result_dict = dict(result)
        # Books와 Food는 반품 없음 → 원래 매출 그대로
        assert "Books" in result_dict
        assert "Food" in result_dict


class TestHard:
    def test_category_diversity_returns_list(self, customer_category_rdd):
        result = category_diversity(customer_category_rdd, 5)
        assert isinstance(result, list)

    def test_category_diversity_range(self, customer_category_rdd):
        result = category_diversity(customer_category_rdd, 5)
        for _, score in result:
            assert 0.0 <= score <= 1.0

    def test_category_diversity_c001(self, customer_category_rdd):
        result = category_diversity(customer_category_rdd, 5)
        result_dict = dict(result)
        # C001은 Electronics, Clothing, Books → 3/5 = 0.6
        assert abs(result_dict["C001"] - 0.6) < 0.01

    def test_category_diversity_sorted_desc(self, customer_category_rdd):
        result = category_diversity(customer_category_rdd, 5)
        scores = [s for _, s in result]
        assert scores == sorted(scores, reverse=True)
