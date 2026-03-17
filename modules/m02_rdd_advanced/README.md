# M02: RDD 심화 (Pair RDD)

## 개념 설명

### Pair RDD란?

Pair RDD는 (키, 값) 튜플로 구성된 RDD입니다. 키-값 연산(집계, 조인 등)에 특화되어 있습니다.

```python
# 일반 RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Pair RDD
pair_rdd = sc.parallelize([("apple", 3), ("banana", 5), ("apple", 2)])
```

### reduceByKey vs groupByKey

**reduceByKey (권장):**
```
파티션1: [("A",1), ("B",1), ("A",1)]
  → 맵 사이드 집계: [("A",2), ("B",1)]
  → 셔플 후 집계: [("A",2), ("B",1)]
```

**groupByKey (비효율):**
```
파티션1: [("A",1), ("B",1), ("A",1)]
  → 셔플 없이 전송: [("A",[1,1,1,...]), ("B",[1,1,...])]
  → 그룹화 후 집계
```

→ reduceByKey는 셔플 전에 파티션 내에서 먼저 집계하여 네트워크 트래픽을 줄입니다.

### combineByKey

가장 유연한 집계 API. 타입 변환이 필요할 때 사용합니다.

```python
# 카테고리별 고유 상품 집합 만들기
rdd.combineByKey(
    createCombiner=lambda v: {v},        # 첫 값: set 초기화
    mergeValue=lambda acc, v: acc | {v}, # 같은 파티션: set에 추가
    mergeCombiners=lambda a, b: a | b,   # 파티션 간 합치기
)
```

## 주요 API

| API | 설명 |
|-----|------|
| `reduceByKey(func)` | 키별 값 집계 (맵 사이드 최적화) |
| `groupByKey()` | 키별 값 그룹화 |
| `countByKey()` | 키별 등장 횟수 (Action, dict 반환) |
| `mapValues(func)` | 값에만 함수 적용 |
| `join(other)` | Inner join |
| `leftOuterJoin(other)` | Left outer join |
| `combineByKey(c, m, mc)` | 유연한 집계 |

## 참고 링크

- [PySpark Pair RDD 문서](https://spark.apache.org/docs/3.5.3/api/python/reference/api/pyspark.RDD.html)
- [RDD 프로그래밍 가이드 - Key-Value Pairs](https://spark.apache.org/docs/3.5.3/rdd-programming-guide.html#working-with-key-value-pairs)
