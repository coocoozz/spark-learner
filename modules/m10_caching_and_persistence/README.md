# M10: 캐싱과 영속성

## 개념 설명

### cache vs persist

```python
df.cache()  # = persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY)  # 명시적 레벨 지정
df.unpersist()  # 캐시 해제
```

### StorageLevel 비교

| Level | 메모리 | 디스크 | 직렬화 | 속도 |
|-------|--------|--------|--------|------|
| MEMORY_ONLY | O | X | X | 가장 빠름 |
| MEMORY_AND_DISK | O | O | X | 중간 |
| DISK_ONLY | X | O | X | 느림 |
| MEMORY_ONLY_SER | O | X | O | 메모리 절약 |

### 언제 캐시?

✅ 캐시 사용:
- 동일한 DataFrame을 여러 번 재사용
- 반복 알고리즘 (MLlib)
- 복잡한 집계 후 다양한 분석

❌ 캐시 불필요:
- 한 번만 사용하는 DataFrame
- 데이터가 메모리보다 훨씬 클 때

### checkpoint vs persist

| | persist | checkpoint |
|-|---------|-----------|
| Lineage | 유지 | 차단 |
| 저장 위치 | 메모리/디스크 | 디스크 |
| 실패 복구 | Lineage로 재계산 | 저장된 파일에서 복구 |

```python
sc.setCheckpointDir("/tmp/checkpoints")
df = df.checkpoint()  # lineage 차단 후 새 DataFrame 반환
```

## 참고 링크

- [RDD Persistence 가이드](https://spark.apache.org/docs/3.5.3/rdd-programming-guide.html#rdd-persistence)
- [DataFrame checkpoint](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html)
