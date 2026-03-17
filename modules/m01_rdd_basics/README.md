# M01: RDD 기초

## 개념 설명

### RDD란?

RDD(Resilient Distributed Dataset)는 Spark의 핵심 데이터 구조입니다.

- **Resilient (탄력적)**: 장애 발생 시 자동으로 복구됩니다 (lineage 기반)
- **Distributed (분산)**: 클러스터의 여러 노드에 분산 저장됩니다
- **Dataset (데이터셋)**: 레코드의 컬렉션입니다

```python
# 비유: RDD는 여러 창고에 나눠 보관된 상자들의 목록
# 각 창고(파티션)에 상자(요소)들이 있고,
# Spark는 모든 창고에서 동시에 작업합니다
```

### Transformation vs Action

| 구분 | 특징 | 예시 |
|------|------|------|
| Transformation | 지연 실행, 새 RDD 반환 | `map`, `filter`, `distinct`, `reduceByKey` |
| Action | 즉시 실행, 결과 반환 | `count`, `collect`, `take`, `save` |

**Lazy Evaluation (지연 평가)**:
- Transformation은 호출 즉시 실행되지 않습니다
- Action이 호출될 때 DAG(Directed Acyclic Graph)를 따라 한 번에 실행됩니다
- 장점: 불필요한 중간 계산을 최적화할 수 있습니다

```python
rdd = sc.textFile("logs.txt")      # Transformation (아직 안 읽음)
filtered = rdd.filter(...)          # Transformation (아직 안 필터링)
result = filtered.count()           # Action → 이 시점에 모든 작업 실행!
```

### Narrow vs Wide Transformation

```
Narrow (셔플 없음):          Wide (셔플 발생):
map, filter, flatMap         reduceByKey, groupByKey
                             sortBy, join, distinct
파티션 1 → 파티션 1          파티션 1 ──┬─→ 파티션 A
파티션 2 → 파티션 2          파티션 2 ──┼─→ 파티션 B
파티션 3 → 파티션 3          파티션 3 ──┘─→ 파티션 C
```

## 주요 API

| API | 종류 | 설명 |
|-----|------|------|
| `sc.textFile(path)` | 생성 | 텍스트 파일 → RDD |
| `rdd.map(func)` | Transformation | 각 요소 변환 |
| `rdd.filter(func)` | Transformation | 조건으로 필터링 |
| `rdd.distinct()` | Transformation | 중복 제거 |
| `rdd.reduceByKey(func)` | Transformation | 키별 값 집계 |
| `rdd.sortBy(func)` | Transformation | 정렬 |
| `rdd.count()` | Action | 요소 수 반환 |
| `rdd.collect()` | Action | 전체 리스트 반환 |
| `rdd.take(n)` | Action | 상위 n개 반환 |

## 문제 설명

### 데이터: Apache CLF(Common Log Format)

```
192.168.1.1 - frank [10/Oct/2023:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326
^IP^          ^user^ ^------timestamp------^       ^---request---^           ^sc^ ^bytes^
[0]                  [3] (대괄호 포함)                                        [-2] [-1]
```

- `line.split(" ")[0]` → IP 주소
- `line.split("[")[1].split(":")[1]` → 시간(hour)
- `line.split()[-2]` → HTTP 상태 코드
- `line.split()[-1]` → 전송 바이트 수

### [Easy] count_lines_and_ips
총 라인 수와 고유 IP 수를 반환합니다.
- 힌트: `sc.textFile` → `count()` / `map(IP추출)` → `distinct()` → `count()`

### [Easy] filter_by_status
특정 HTTP 상태 코드의 로그 라인을 필터링합니다.
- 힌트: `filter(lambda line: f" {status_code} " in line)`

### [Medium] top_n_ips
IP별 요청 횟수 상위 N개를 반환합니다.
- 힌트: `map((ip, 1))` → `reduceByKey(+)` → `sortBy(count, desc)` → `take(n)`

### [Medium] requests_by_hour
시간대별 요청 수를 집계합니다.
- 힌트: 타임스탬프 파싱 후 `(hour, 1)` → `reduceByKey` → `sortBy(hour)`

### [Hard] bytes_by_status
상태 코드별 총 바이트 수를 집계합니다.
- 힌트: `split()[-2]` (상태코드), `split()[-1]` (바이트) 파싱 → `reduceByKey`

## 참고 링크

- [PySpark RDD API 공식 문서](https://spark.apache.org/docs/3.5.3/api/python/reference/api/pyspark.RDD.html)
- [RDD 프로그래밍 가이드](https://spark.apache.org/docs/3.5.3/rdd-programming-guide.html)
