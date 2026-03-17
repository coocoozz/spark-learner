# M05: 조인과 집계

## 개념 설명

### Join 종류

```
customers: C001-Alice, C002-Bob, C003-Carol
orders:    O001-C001, O002-C002, O004-C001

INNER JOIN:   C001-Alice-O001, C001-Alice-O004, C002-Bob-O002
LEFT OUTER:   위 + C003-Carol-NULL
RIGHT OUTER:  (orders가 오른쪽이면 주문 없는 고객 제외)
FULL OUTER:   양쪽 모두 포함
```

### 집계 함수

```python
from pyspark.sql.functions import sum, avg, count, countDistinct, min, max

df.groupBy("category").agg(
    sum("amount").alias("total"),
    avg("amount").alias("average"),
    count("order_id").alias("orders"),
    countDistinct("customer_id").alias("unique_customers"),
)
```

### 성능 팁

- **broadcast join**: 작은 테이블(< 10MB)은 broadcast로 셔플 제거
- **조인 순서**: 가장 큰 테이블을 왼쪽으로, 작은 테이블을 오른쪽으로
- **필터 먼저**: 조인 전에 불필요한 행을 필터링하면 셔플 크기 감소

## 참고 링크

- [Spark SQL Join 가이드](https://spark.apache.org/docs/3.5.3/sql-ref-syntax-qry-select-join.html)
- [집계 함수](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/functions.html)
