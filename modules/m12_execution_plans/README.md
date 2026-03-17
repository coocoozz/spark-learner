# M12: 실행 계획 분석

## 개념 설명

### 실행 계획 단계

```
SQL/API
  ↓ Parsing
Parsed Logical Plan
  ↓ Analysis (이름 해석, 타입 체크)
Analyzed Logical Plan
  ↓ Catalyst Optimization
Optimized Logical Plan
  ↓ Physical Planning
Physical Plan → 실행
```

### explain() 사용

```python
df.explain()                    # 물리 계획 (기본)
df.explain("extended")          # 논리 + 물리
df.explain("formatted")         # 가독성 높은 형식
df.explain("cost")              # 비용 정보

# 문자열로 얻기 (내부 API)
plan = df._jdf.queryExecution().simpleString()
```

### 주요 노드 설명

| 노드 | 의미 |
|------|------|
| `Filter` | WHERE 조건 |
| `Project` | SELECT 컬럼 선택 |
| `Exchange` | 셔플 (파티션 재분배) |
| `HashAggregate` | 집계 (GROUP BY) |
| `BroadcastHashJoin` | Broadcast 조인 |
| `SortMergeJoin` | Sort Merge 조인 |

### Predicate Pushdown

```
비최적화: 전체 파일 읽기 → Filter
최적화:  Filter 먼저 → 필요한 데이터만 읽기 (PushedFilters)
```

Parquet에서는 row group 통계로 불필요한 블록 스킵

## 참고 링크

- [Catalyst Optimizer](https://spark.apache.org/docs/3.5.3/sql-ref.html)
- [explain() API](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)
