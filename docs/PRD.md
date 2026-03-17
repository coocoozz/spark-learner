# Spark-Learner PRD (Product Requirements Document)

## Context

PySpark 3.5.3을 체계적으로 학습하기 위한 문제 기반 TDD 프로젝트. 각 토픽별로 파이프라인 스타일의 TODO 함수가 주어지고, 학습자가 구현한 뒤 pytest로 정답을 검증하는 구조. 입문부터 중급까지 Core + SQL + 성능 튜닝을 다룬다.

---

## 1. 환경 설정

- **Python**: 3.12 (PySpark 3.5.3은 Python 3.13 미지원 → `.python-version` 및 `pyproject.toml` 수정)
- **의존성**: `pyspark==3.5.3`, `pytest>=7.0`, `faker>=20.0`
- **실행**: `local[*]` 모드, `uv run pytest`로 검증
- **Java**: JDK 11 또는 17 필요

## 2. 디렉토리 구조

```
spark-learner/
├── pyproject.toml
├── .python-version              # 3.12
├── conftest.py                  # SparkSession fixture (공용)
├── data/                        # 샘플 데이터 파일
│   ├── ecommerce/               # orders.csv, products.csv, customers.json
│   ├── logs/                    # access_logs.txt
│   └── finance/                 # stock_prices.csv, transactions.parquet
├── src/helpers/
│   └── data_generator.py        # Faker 기반 데이터 생성 유틸
├── modules/
│   ├── m01_rdd_basics/          # 각 모듈: README.md + problems.py + test_problems.py + solutions.py
│   ├── m02_rdd_advanced/
│   ├── m03_dataframe_basics/
│   ├── m04_dataframe_operations/
│   ├── m05_joins_and_aggregations/
│   ├── m06_window_functions/
│   ├── m07_udf/
│   ├── m08_spark_sql/
│   ├── m09_partitioning/
│   ├── m10_caching_and_persistence/
│   ├── m11_shuffle_optimization/
│   └── m12_execution_plans/
└── (solutions.py는 각 모듈 디렉토리 내에 포함)
```

## 3. 모듈 구성 (12개 모듈)

각 모듈 = `README.md`(개념+문제설명+힌트) + `problems.py`(TODO 함수) + `test_problems.py`(pytest 검증) + `solutions.py`(정답+상세 설명)

### Phase 1: Core 기초

**M01: RDD 기초** | 도메인: 서버 액세스 로그 | 데이터: `data/logs/access_logs.txt`
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | 총 라인 수와 고유 IP 수 반환 | `count_lines_and_ips(sc, path)` |
| Easy | 특정 HTTP 상태 코드 필터링 | `filter_by_status(sc, path, status_code)` |
| Medium | IP별 요청 횟수 상위 N개 | `top_n_ips(sc, path, n)` |
| Medium | 시간대별 요청 수 집계 | `requests_by_hour(sc, path)` |
| Hard | 로그 파싱 + 상태코드별 총 바이트 수 | `bytes_by_status(sc, path)` |

**M02: RDD 심화 (Pair RDD)** | 도메인: 이커머스 주문 | 데이터: Faker 생성
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | 카테고리별 총 매출 (reduceByKey) | `revenue_by_category(rdd)` |
| Easy | 유저별 주문 건수 (countByKey) | `orders_per_user(rdd)` |
| Medium | 카테고리별 매출 상위 N상품 (groupByKey) | `top_products_per_category(rdd, n)` |
| Medium | 주문-반품 join으로 순매출 | `net_revenue(orders_rdd, returns_rdd)` |
| Hard | 구매 카테고리 다양성 지수 (combineByKey) | `category_diversity(rdd, total_categories)` |

**M03: DataFrame 기초** | 도메인: 이커머스 | 데이터: CSV/JSON 파일
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | CSV 읽기 + 스키마 확인 | `load_and_describe(spark, path)` |
| Easy | 명시적 StructType으로 JSON 읽기 | `load_with_schema(spark, path, schema)` |
| Medium | Python dict에서 DataFrame 생성 | `create_from_dicts(spark, data, schema)` |
| Medium | 컬럼 타입 캐스팅 + unionByName | `safe_union(df1, df2)` |
| Hard | 중첩 JSON 평탄화 | `flatten_nested(df)` |

**M04: DataFrame 연산** | 도메인: 이커머스 주문 | 데이터: CSV + Faker
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | withColumn으로 total 컬럼 추가 | `add_total_column(df)` |
| Easy | filter + 정렬 | `filter_and_sort(df, min_amount, sort_col)` |
| Medium | when/otherwise 다중 조건 분류 | `categorize_orders(df)` |
| Medium | 날짜 파싱 + 배송기간 계산 | `calculate_delivery_days(df)` |
| Hard | regexp_extract로 비정형 컬럼 정제 | `clean_address_column(df)` |

### Phase 2: Core 심화

**M05: 조인과 집계** | 도메인: 이커머스 다중 테이블 | 데이터: CSV 3종 + Faker
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | inner join + groupBy 매출 집계 | `revenue_by_customer(orders_df, customers_df)` |
| Easy | left outer join으로 주문 없는 고객 | `customers_without_orders(customers_df, orders_df)` |
| Medium | 3테이블 join + 교차 집계 | `cross_summary(orders, customers, products)` |
| Medium | 다중 agg (총매출, 평균, 건수, 고유고객수) | `category_metrics(df)` |
| Hard | self-join 연속 주문 간격 계산 | `order_intervals(orders_df)` |

**M06: 윈도우 함수** | 도메인: 금융 주가 | 데이터: `data/finance/stock_prices.csv`
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | 종목별 종가 순위 (rank) | `rank_by_price(df)` |
| Easy | 종목별 누적 거래량 | `cumulative_volume(df)` |
| Medium | 이동평균 (5일, 20일) | `moving_averages(df, windows)` |
| Medium | 전일 대비 수익률 + 연속 상승일 | `daily_returns_and_streaks(df)` |
| Hard | 최대 낙폭(drawdown) 구간 식별 | `max_drawdown(df)` |

**M07: UDF** | 도메인: 이커머스 텍스트 | 데이터: Faker 생성
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | 문자열 정규화 UDF | `register_normalize_udf(spark)` |
| Medium | 주소 파싱 UDF (구조체 반환) | `register_address_parser_udf(spark)` |
| Medium | 감성 점수 UDF | `register_sentiment_udf(spark)` |
| Hard | Pandas UDF - 카테고리별 Z-score | `normalize_by_category(df, col_name)` |

### Phase 3: SQL

**M08: Spark SQL** | 도메인: 이커머스 통합 | 데이터: 기존 CSV → temp view
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | temp view + SQL 필터링/정렬 | `query_top_orders(spark, df, n)` |
| Easy | GROUP BY + HAVING | `high_value_categories(spark, df, threshold)` |
| Medium | 서브쿼리: 평균 이상 주문 고객 | `above_average_customers(spark, orders_df, customers_df)` |
| Medium | CASE WHEN + JOIN SQL | `customer_segments_sql(spark, orders_df, customers_df)` |
| Hard | CTE + 윈도우 함수 결합 월별 리포트 | `monthly_growth_report(spark, orders_df)` |

### Phase 4: Performance

**M09: 파티셔닝** | 도메인: 대규모 로그 | 데이터: Faker 10만+행
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | repartition / coalesce 조정 | `adjust_partitions(df, n, use_coalesce)` |
| Easy | partitionBy로 Parquet 저장 | `save_partitioned(df, path, col)` |
| Medium | 파티션 키별 skew 분석 | `analyze_partition_skew(df, col)` |
| Hard | salting으로 데이터 스큐 해소 | `salt_skewed_key(df, skew_col, num_salts)` |

**M10: 캐싱과 영속성** | 도메인: 금융 거래 | 데이터: Parquet + Faker
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | cache / unpersist + isCached 확인 | `cache_and_verify(df)` |
| Easy | StorageLevel 비교 | `persist_with_level(df, level)` |
| Medium | cache 유무 성능 비교 | `measure_cache_benefit(spark, df)` |
| Hard | checkpoint로 lineage 차단 | `iterative_with_checkpoint(spark, df, iterations)` |

**M11: 셔플 최적화** | 도메인: 대규모 조인 | 데이터: Faker (큰 팩트 + 작은 룩업)
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | broadcast join | `broadcast_join(large_df, small_df, key)` |
| Medium | 셔플 파티션 수 조정 비교 | `optimize_shuffle_partitions(spark, df, query_func)` |
| Medium | 사전 파티셔닝으로 반복 join 최적화 | `pre_partitioned_joins(df1, df2, key, n)` |
| Hard | 스큐 조인 최적화 (salting/AQE) | `handle_skew_join(skewed_df, normal_df, key)` |

**M12: 실행 계획 분석** | 도메인: 통합 | 데이터: 기존 재활용
| 난이도 | 문제 | 함수 |
|--------|------|------|
| Easy | explain 파싱 - 노드 식별 | `identify_plan_nodes(df)` |
| Medium | predicate pushdown 전후 비교 | `compare_pushdown(spark, path, filter_col, value)` |
| Medium | join 전략 식별 | `identify_join_strategy(df)` |
| Hard | 비효율 쿼리 최적화 | `optimize_query(spark, df1, df2, df3)` |

## 4. 공통 컨벤션

### README.md 패턴 (각 모듈)
각 모듈의 README.md는 초보자도 이해할 수 있도록 다음을 포함:
- **개념 설명**: 해당 토픽의 핵심 개념을 예시와 함께 상세히 설명
- **주요 API 정리**: 사용할 PySpark API 목록과 간단한 사용법
- **문제별 요구사항**: 각 문제의 상세 설명, 입출력 예시
- **힌트**: 난이도별 힌트 (Easy: 거의 정답에 가까운 힌트, Hard: 방향성만 제시)
- **참고 링크**: PySpark 공식 문서 링크

### problems.py 패턴
```python
def function_name(spark, ...):
    """
    [Easy] 문제 제목

    문제 설명을 상세히 작성합니다.
    초보자도 이해할 수 있도록 배경 지식과 예시를 포함합니다.

    Args:
        spark: SparkSession 인스턴스
        ...

    Returns:
        반환값 설명 + 예시

    Hint:
        - 사용할 API: sc.textFile(), rdd.count(), rdd.distinct()
        - 접근법: 먼저 파일을 읽고, 각 라인에서 IP를 추출한 뒤...
    """
    # TODO: 구현하세요
    raise NotImplementedError
```

### solutions.py 패턴 (각 모듈 - 필수)
```python
"""
M01: RDD 기초 - 솔루션

각 문제의 정답 코드와 상세한 설명을 포함합니다.
학습자가 구현한 후 비교하거나, 막혔을 때 참고할 수 있습니다.
"""

def count_lines_and_ips(sc, path):
    """
    풀이 설명:

    1단계: sc.textFile()로 로그 파일을 RDD로 읽습니다.
       - textFile은 각 줄을 하나의 요소로 하는 RDD를 생성합니다.

    2단계: count()로 전체 라인 수를 구합니다.
       - count()는 RDD의 요소 수를 반환하는 '액션(action)'입니다.

    3단계: 각 라인에서 IP 주소를 추출합니다.
       - Apache CLF 형식에서 IP는 첫 번째 공백 전 문자열입니다.

    4단계: distinct()로 중복을 제거하고 count()로 셉니다.

    핵심 개념:
    - Transformation vs Action: map/distinct는 변환(지연 실행),
      count는 액션(즉시 실행)
    - Lazy Evaluation: 변환은 액션이 호출될 때까지 실행되지 않음
    """
    rdd = sc.textFile(path)
    total_lines = rdd.count()
    unique_ips = rdd.map(lambda line: line.split(" ")[0]).distinct().count()
    return (total_lines, unique_ips)
```

솔루션 작성 규칙:
- **모든 문제에 솔루션 필수 제공**
- 코드 한 줄마다 왜 이렇게 작성했는지 설명
- 사용된 PySpark API의 동작 원리 설명 (Transformation vs Action 등)
- 초보자가 이해하기 어려운 개념은 비유나 예시로 보충
- 대안적 풀이가 있으면 함께 제시 ("다른 방법으로는...")
- Hard 문제는 단계별로 사고 과정을 풀어서 설명

### test_problems.py 패턴
```python
class TestEasy:
    def test_xxx(self, spark): ...
class TestMedium:
    def test_xxx(self, spark): ...
class TestHard:
    def test_xxx(self, spark): ...
```

### conftest.py (최상위)
- `spark` fixture: `scope="session"`, `local[*]`, shuffle.partitions=4, UI disabled
- `sc` fixture: `spark.sparkContext`

### 데이터 생성
- Faker 사용 시 seed 고정 → 테스트 기대값 결정론적
- `src/helpers/data_generator.py`에 공용 생성 함수 집중

## 5. 실행 방법

```bash
uv run pytest                                    # 전체
uv run pytest modules/m01_rdd_basics/            # 특정 모듈
uv run pytest -k "TestEasy"                      # 난이도별
uv run pytest --tb=short -q                      # 진행상황 요약
```

## 6. 구현 순서

1. 환경 설정 (`.python-version` → 3.12, `pyproject.toml` 수정, 의존성 설치)
2. 프로젝트 뼈대 (디렉토리, `conftest.py`, `data_generator.py`)
3. 샘플 데이터 파일 생성
4. M01 ~ M12 순차 구현 (각 모듈: README.md → problems.py → test_problems.py → solutions.py)
5. README.md 프로젝트 가이드

## 7. 주의사항

- PySpark 3.5.3은 Python 3.13 미지원 → 반드시 3.12 사용
- JDK 11/17 필요 (README에 안내)
- 로컬 모드에서 성능 차이 체감 제한 → `explain()` 기반 논리적 검증 중심
- SparkSession `scope="session"`으로 재사용, temp view 이름 충돌 주의
- Faker seed 고정으로 테스트 결정론성 보장

## 8. 검증 방법

1. `uv run pytest` 전체 실행 → 모든 테스트가 `NotImplementedError`로 실패하는지 확인
2. 각 모듈별 솔루션 구현 후 `uv run pytest modules/mXX/` → 통과 확인
3. `uv run pytest -k "TestEasy"` → Easy부터 순차적으로 풀어나가며 검증
