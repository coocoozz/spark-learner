# M09: 파티셔닝

## 개념 설명

### repartition vs coalesce

| | repartition(n) | coalesce(n) |
|-|---------------|-------------|
| 셔플 | O (발생) | X (없음) |
| 방향 | 증가/감소 모두 | 감소만 |
| 분포 | 균등 | 불균등 가능 |
| 사용 시기 | 균등 분배 필요 | 파일 저장 전 축소 |

### partitionBy로 저장

```python
df.write.partitionBy("category").mode("overwrite").parquet(path)
# 결과:
# path/category=Electronics/part-00000.parquet
# path/category=Clothing/part-00000.parquet
```

읽을 때 Partition Pruning 자동 적용:
```python
spark.read.parquet(path).filter("category = 'Electronics'")
# Electronics 디렉토리만 스캔
```

### Salting으로 스큐 해소

```python
from pyspark.sql.functions import concat, lit, rand

# Electronics 70%를 4개로 분산
salt = (rand() * 4).cast("int").cast("string")
df.withColumn("salted_key", concat(col("category"), lit("_"), salt))
# "Electronics_0", "Electronics_1", "Electronics_2", "Electronics_3"
```

## 참고 링크

- [Spark 파티셔닝 가이드](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html)
- [AQE Skew Join](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#adaptive-query-execution)
