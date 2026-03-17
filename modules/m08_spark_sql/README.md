# M08: Spark SQL

## 개념 설명

### Temp View 등록

```python
df.createOrReplaceTempView("orders")    # 현재 세션
df.createOrReplaceGlobalTempView("orders")  # 전역 (global_temp.orders)
```

### SQL 실행

```python
result = spark.sql("""
    SELECT category, SUM(amount) AS total
    FROM orders
    GROUP BY category
    HAVING SUM(amount) > 1000
    ORDER BY total DESC
""")
```

### CTE (Common Table Expression)

```sql
WITH monthly AS (
    SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS month,
           SUM(amount) AS revenue
    FROM orders
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
),
ranked AS (
    SELECT *, LAG(revenue) OVER (ORDER BY month) AS prev_revenue
    FROM monthly
)
SELECT *, (revenue - prev_revenue) / prev_revenue * 100 AS growth
FROM ranked
```

### SQL vs DataFrame API

| 기능 | SQL | DataFrame API |
|------|-----|--------------|
| 가독성 | 높음 | 코드 스타일 |
| 서브쿼리 | 자연스러움 | 복잡 |
| 최적화 | 동일 (Catalyst) | 동일 |
| 타입 안전성 | 런타임 체크 | 컴파일 타임 |

## 참고 링크

- [Spark SQL 가이드](https://spark.apache.org/docs/3.5.3/sql-programming-guide.html)
- [내장 함수 목록](https://spark.apache.org/docs/3.5.3/sql-ref-functions-builtin.html)
