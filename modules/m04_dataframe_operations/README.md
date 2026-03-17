# M04: DataFrame 연산

## 개념 설명

### withColumn과 컬럼 함수

```python
from pyspark.sql.functions import col, when, to_date, datediff, regexp_extract

# 컬럼 참조 방법
df["column_name"]       # df 인스턴스로 접근
col("column_name")      # 함수로 접근 (더 유연)
```

### when / otherwise (CASE WHEN)

```python
# SQL: CASE WHEN amount >= 500 THEN 'Premium' ELSE 'Basic' END
from pyspark.sql.functions import when

tier = when(col("amount") >= 500, "Premium") \
       .when(col("amount") >= 100, "Standard") \
       .otherwise("Basic")

df.withColumn("tier", tier)
```

### 날짜 처리

```python
from pyspark.sql.functions import to_date, datediff, date_add, months_between

# 문자열 → 날짜
df.withColumn("dt", to_date(col("date_str"), "yyyy-MM-dd"))

# 날짜 차이 (일)
df.withColumn("days", datediff(col("end_date"), col("start_date")))
```

### 정규식

```python
from pyspark.sql.functions import regexp_extract, regexp_replace

# 패턴 추출: 5자리 숫자 추출
df.withColumn("zip", regexp_extract(col("address"), r"(\d{5})", 1))

# 패턴 대체
df.withColumn("clean", regexp_replace(col("text"), r"[^a-zA-Z0-9]", ""))
```

## 주요 API

| API | 설명 |
|-----|------|
| `withColumn(name, expr)` | 컬럼 추가/수정 |
| `filter(condition)` / `where(condition)` | 행 필터링 |
| `orderBy(col)` / `sort(col)` | 정렬 |
| `when(cond, val).otherwise(val)` | 조건부 값 |
| `to_date(col, format)` | 문자열 → 날짜 |
| `datediff(end, start)` | 날짜 차이 (일) |
| `regexp_extract(col, pattern, idx)` | 정규식 추출 |

## 참고 링크

- [PySpark Functions API](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/functions.html)
- [SQL Functions Guide](https://spark.apache.org/docs/3.5.3/sql-ref-functions.html)
