"""
공용 Faker 기반 데이터 생성 유틸리티.
Faker seed를 고정하여 테스트 결정론성을 보장합니다.
"""
from __future__ import annotations

import random
from datetime import date, timedelta

from faker import Faker

SEED = 42
fake = Faker("ko_KR")
Faker.seed(SEED)
random.seed(SEED)

CATEGORIES = ["Electronics", "Clothing", "Books", "Food", "Sports"]
STATUSES = ["delivered", "cancelled", "processing", "shipped"]


def generate_orders(n: int = 100) -> list[dict]:
    """주문 데이터 생성 (M02, M04, M05, M07, M08 등에 사용)."""
    orders = []
    start = date(2023, 1, 1)
    for i in range(n):
        order_date = start + timedelta(days=random.randint(0, 364))
        delivery_date = order_date + timedelta(days=random.randint(1, 14))
        category = random.choice(CATEGORIES)
        amount = round(random.uniform(10.0, 2000.0), 2)
        orders.append({
            "order_id": f"O{i+1:04d}",
            "customer_id": f"C{random.randint(1, 20):03d}",
            "product_id": f"P{random.randint(1, 50):03d}",
            "category": category,
            "amount": amount,
            "quantity": random.randint(1, 10),
            "order_date": order_date.isoformat(),
            "delivery_date": delivery_date.isoformat(),
            "status": random.choice(STATUSES),
        })
    return orders


def generate_returns(orders: list[dict], return_rate: float = 0.1) -> list[dict]:
    """반품 데이터 생성 (M02 순매출 계산용)."""
    returns = []
    for order in orders:
        if random.random() < return_rate:
            returns.append({
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "category": order["category"],
                "refund_amount": round(order["amount"] * random.uniform(0.5, 1.0), 2),
            })
    return returns


def generate_customers(n: int = 20) -> list[dict]:
    """고객 데이터 생성."""
    customers = []
    for i in range(n):
        customers.append({
            "customer_id": f"C{i+1:03d}",
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "age": random.randint(18, 65),
        })
    return customers


def generate_products(n: int = 50) -> list[dict]:
    """상품 데이터 생성."""
    products = []
    for i in range(n):
        category = random.choice(CATEGORIES)
        products.append({
            "product_id": f"P{i+1:03d}",
            "name": fake.catch_phrase(),
            "category": category,
            "price": round(random.uniform(5.0, 3000.0), 2),
            "stock": random.randint(0, 500),
        })
    return products


def generate_reviews(n: int = 200) -> list[dict]:
    """상품 리뷰 데이터 생성 (M07 UDF 실습용)."""
    reviews = []
    for i in range(n):
        score = random.randint(1, 5)
        reviews.append({
            "review_id": f"R{i+1:04d}",
            "product_id": f"P{random.randint(1, 50):03d}",
            "customer_id": f"C{random.randint(1, 20):03d}",
            "score": score,
            "text": fake.sentence(nb_words=random.randint(5, 20)),
            "category": random.choice(CATEGORIES),
            "address_raw": f"{fake.road_address()} {fake.postcode()}",
        })
    return reviews


def generate_large_orders(n: int = 100_000) -> list[dict]:
    """대규모 주문 데이터 생성 (M09, M11 파티셔닝/셔플 실습용)."""
    orders = []
    start = date(2022, 1, 1)
    # 스큐 시뮬레이션: 특정 카테고리에 데이터 편중
    skewed_category = "Electronics"
    for i in range(n):
        order_date = start + timedelta(days=random.randint(0, 729))
        if random.random() < 0.7:
            category = skewed_category
        else:
            category = random.choice(CATEGORIES)
        orders.append({
            "order_id": f"O{i+1:07d}",
            "customer_id": f"C{random.randint(1, 1000):04d}",
            "category": category,
            "amount": round(random.uniform(10.0, 2000.0), 2),
            "order_date": order_date.isoformat(),
            "region": random.choice(["Seoul", "Busan", "Daegu", "Incheon", "Daejeon"]),
        })
    return orders


def generate_transactions(n: int = 500) -> list[dict]:
    """금융 거래 데이터 생성 (M10 캐싱 실습용)."""
    transactions = []
    start = date(2023, 1, 1)
    for i in range(n):
        tx_date = start + timedelta(days=random.randint(0, 364))
        transactions.append({
            "tx_id": f"T{i+1:05d}",
            "account_id": f"A{random.randint(1, 100):03d}",
            "amount": round(random.uniform(-5000.0, 10000.0), 2),
            "tx_type": random.choice(["deposit", "withdrawal", "transfer", "fee"]),
            "tx_date": tx_date.isoformat(),
            "category": random.choice(["salary", "food", "transport", "shopping", "utilities"]),
        })
    return transactions
