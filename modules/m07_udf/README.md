# M07: UDF (User Defined Functions)

## 개념 설명

### UDF 종류

| 종류 | 처리 단위 | 성능 | 사용 시기 |
|------|----------|------|----------|
| Python UDF | 행(row) | 느림 | 간단한 변환 |
| Pandas UDF | 배치(Series) | 빠름 | 수치 계산 |
| SQL UDF | 행 | 중간 | SQL에서 사용 |

### Python UDF 등록

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 방법 1: udf() 함수
normalize = udf(lambda text: text.lower().strip(), StringType())

# 방법 2: 데코레이터
@udf(returnType=StringType())
def normalize(text: str) -> str:
    return text.lower().strip()

# 적용
df.withColumn("normalized", normalize(col("text")))
```

### Pandas UDF (Vectorized)

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def zscore(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std()

# Window와 함께 사용
w = Window.partitionBy("category")
df.withColumn("zscore", zscore(col("amount")).over(w))
```

### UDF 성능 주의사항

- **UDF는 Catalyst Optimizer 최적화 불가** → 가능하면 내장 함수 사용
- Python UDF는 JVM ↔ Python 직렬화 오버헤드 큼
- Pandas UDF는 Apache Arrow를 사용해 배치로 처리 → 10~100x 빠름

## 참고 링크

- [PySpark UDF 가이드](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/functions.html)
- [Pandas UDF 블로그](https://spark.apache.org/docs/3.5.3/sql-pyspark-pandas-with-arrow.html)
