# M03: DataFrame 기초

## 개념 설명

### DataFrame이란?

DataFrame은 이름이 있는 컬럼으로 구성된 분산 데이터 컬렉션입니다. RDD에 스키마가 추가된 형태입니다.

- **스키마**: 컬럼명과 타입 정보를 가집니다
- **Catalyst Optimizer**: SQL 최적화 엔진이 쿼리를 최적화합니다
- **Tungsten**: 효율적인 메모리 관리와 코드 생성

### SparkSession vs SparkContext

```python
# SparkSession: DataFrame API의 진입점 (Spark 2.0+)
spark = SparkSession.builder.getOrCreate()

# SparkContext: RDD API의 진입점
sc = spark.sparkContext
```

### 데이터 읽기 옵션

```python
# CSV
df = spark.read.csv(path, header=True, inferSchema=True)
df = spark.read.option("header", True).option("inferSchema", True).csv(path)

# JSON Lines
df = spark.read.json(path)
df = spark.read.json(path, schema=schema)  # 명시적 스키마

# Parquet (스키마 내장)
df = spark.read.parquet(path)
```

### StructType으로 스키마 정의

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])
```

## 주요 API

| API | 설명 |
|-----|------|
| `spark.read.csv(path, ...)` | CSV 읽기 |
| `spark.read.json(path, schema=...)` | JSON 읽기 |
| `spark.createDataFrame(data, schema)` | Python 데이터 → DataFrame |
| `df.count()` | 행 수 (Action) |
| `df.columns` | 컬럼명 리스트 |
| `df.schema` | StructType 스키마 |
| `df.withColumn(name, expr)` | 컬럼 추가/수정 |
| `df.unionByName(other)` | 컬럼명 기준 합치기 |
| `col("a.b")` | 중첩 컬럼 접근 |

## 참고 링크

- [PySpark DataFrame API](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/dataframe.html)
- [Data Sources](https://spark.apache.org/docs/3.5.3/sql-data-sources.html)
