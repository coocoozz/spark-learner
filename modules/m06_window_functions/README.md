# M06: 윈도우 함수

## 개념 설명

### Window 함수란?

groupBy와 달리 행을 줄이지 않고 새 컬럼을 추가합니다.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, lag, sum, avg, max

w = Window.partitionBy("symbol").orderBy("date")

df.withColumn("rank", rank().over(w))
df.withColumn("prev", lag("close", 1).over(w))
df.withColumn("cumsum", sum("volume").over(w))
```

### Frame 지정

```python
# 처음부터 현재까지 (누적)
Window.rowsBetween(Window.unboundedPreceding, 0)

# 현재 포함 5개 행 (이동평균)
Window.rowsBetween(-4, 0)
```

### 순위 함수 비교

| 함수 | 동점 처리 | 예시 |
|------|----------|------|
| `rank()` | 같은 순위, 다음 건너뜀 | 1, 1, 3 |
| `dense_rank()` | 같은 순위, 연속 | 1, 1, 2 |
| `row_number()` | 고유 순위 | 1, 2, 3 |

## 참고 링크

- [Window Functions 가이드](https://spark.apache.org/docs/3.5.3/sql-ref-syntax-qry-select-window.html)
- [PySpark Window API](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/window.html)
