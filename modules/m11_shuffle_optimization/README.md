# M11: 셔플 최적화

## 개념 설명

### Broadcast Join

```python
from pyspark.sql.functions import broadcast

# 작은 테이블을 broadcast
large_df.join(broadcast(small_df), on="key")

# 자동 threshold (기본 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")
```

### Shuffle Partitions 조정

```python
# 기본값 200 → 작은 데이터에는 과도
spark.conf.set("spark.sql.shuffle.partitions", "4")

# AQE로 자동 조정 (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Salting으로 스큐 조인 해소

```python
# 스큐 테이블: 랜덤 salt 추가
skewed.withColumn("salted_key",
    concat(col("key"), lit("_"), (rand()*4).cast("int").cast("string")))

# 정상 테이블: salt만큼 복제
normal.withColumn("salt", explode(array([lit(0), lit(1), lit(2), lit(3)])))
      .withColumn("salted_key", concat(col("key"), lit("_"), col("salt").cast("string")))
```

## 참고 링크

- [Spark 조인 전략](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html)
- [AQE 가이드](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#adaptive-query-execution)
