# Spark Learner

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-7.0+-0A9EDC?logo=pytest&logoColor=white)
![uv](https://img.shields.io/badge/uv-package%20manager-DE5FE9)

PySpark 3.5.3 TDD 기반 학습 프로젝트입니다. 12개 모듈에 걸쳐 RDD 기초부터 성능 최적화까지, 문제를 직접 구현하고 테스트로 검증하는 방식으로 학습합니다.

---

## 학습 모듈

총 53개 문제, 153개 테스트로 구성되어 있습니다.

| Phase | 모듈 | 주제 | 난이도별 문제 수 |
|-------|------|------|----------------|
| **Phase 1** Core 기초 | M01 | RDD 기초 | Easy 2 / Medium 2 / Hard 1 |
| | M02 | RDD 심화 (Pair RDD) | Easy 2 / Medium 2 / Hard 1 |
| | M03 | DataFrame 기초 | Easy 2 / Medium 2 / Hard 1 |
| | M04 | DataFrame 연산 | Easy 2 / Medium 2 / Hard 1 |
| **Phase 2** Core 심화 | M05 | Join & Aggregation | Easy 2 / Medium 2 / Hard 1 |
| | M06 | Window 함수 | Easy 2 / Medium 2 / Hard 1 |
| | M07 | UDF | Easy 1 / Medium 2 / Hard 1 |
| **Phase 3** SQL | M08 | Spark SQL | Easy 2 / Medium 2 / Hard 1 |
| **Phase 4** 성능 최적화 | M09 | 파티셔닝 | Easy 2 / Medium 1 / Hard 1 |
| | M10 | 캐싱 & 퍼시스턴스 | Easy 2 / Medium 1 / Hard 1 |
| | M11 | 셔플 최적화 | Easy 1 / Medium 2 / Hard 1 |
| | M12 | 실행 계획 분석 | Easy 1 / Medium 2 / Hard 1 |

---

## 사전 요구사항

- **Python 3.12** (`.python-version` 파일로 자동 설정)
- **JDK 11 또는 17** (PySpark 실행에 필요)
- **[uv](https://docs.astral.sh/uv/)** 패키지 매니저

JDK 설치 확인:
```bash
java -version
```

---

## 설치

```bash
git clone https://github.com/your-org/spark-learner.git
cd spark-learner
uv sync
```

---

## 프로젝트 구조

```
spark-learner/
├── modules/
│   ├── m01_rdd_basics/
│   │   ├── README.md         # 개념 설명 + 문제 힌트
│   │   ├── problems.py       # 구현할 함수 (TODO)
│   │   ├── solutions.py      # 참고 풀이
│   │   └── test_problems.py  # 테스트 코드
│   ├── m02_rdd_advanced/
│   └── ...                   # M03 ~ M12 동일 구조
├── data/
│   ├── ecommerce/            # orders.csv, products.csv, customers.json
│   ├── finance/              # stock_prices.csv
│   └── logs/                 # access_logs.txt (Apache CLF)
├── src/helpers/
│   └── data_generator.py     # Faker 기반 테스트 데이터 생성
└── conftest.py               # SparkSession/SparkContext pytest 픽스처
```

---

## 사용법

### 학습 흐름

```
1. modules/mXX_.../README.md  읽기  →  개념 이해 + 힌트 확인
2. modules/mXX_.../problems.py 열기  →  TODO 함수 구현
3. uv run pytest modules/mXX_.../  →  테스트로 검증
```

### 테스트 실행

**전체 테스트 실행:**
```bash
uv run pytest
```

**특정 모듈만 실행:**
```bash
uv run pytest modules/m01_rdd_basics/
```

**난이도별 실행:**
```bash
uv run pytest -k "TestEasy"
uv run pytest -k "TestMedium"
uv run pytest -k "TestHard"
```

**특정 함수 테스트:**
```bash
uv run pytest -k "test_count_lines_total"
```

**상세 출력과 함께:**
```bash
uv run pytest modules/m01_rdd_basics/ -v
```

---

## 예시: M01 첫 번째 문제 풀기

### Step 1. README.md로 개념과 문제 확인

`modules/m01_rdd_basics/README.md`를 읽으면 RDD 개념과 함께 문제별 힌트를 확인할 수 있습니다.

```
### [Easy] count_lines_and_ips
총 라인 수와 고유 IP 수를 반환합니다.
- 힌트: sc.textFile → count() / map(IP추출) → distinct() → count()
```

### Step 2. problems.py에서 TODO 확인

`modules/m01_rdd_basics/problems.py`를 열면 구현할 함수가 있습니다:

```python
def count_lines_and_ips(sc, path: str) -> tuple[int, int]:
    """
    [Easy] 총 라인 수와 고유 IP 수 반환

    Returns:
        (total_lines, unique_ip_count) 튜플
        예: (30, 9)
    """
    # TODO: 구현하세요
    raise NotImplementedError
```

### Step 3. 테스트 실행 (구현 전)

```bash
$ uv run pytest modules/m01_rdd_basics/ -k "test_count" -v

FAILED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_lines_total - NotImplementedError
FAILED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_unique_ips - NotImplementedError
FAILED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_lines_and_ips_returns_tuple - NotImplementedError
```

### Step 4. 구현

`# TODO: 구현하세요` 아래의 `raise NotImplementedError`를 제거하고 구현합니다:

```python
def count_lines_and_ips(sc, path: str) -> tuple[int, int]:
    rdd = sc.textFile(path)
    total_lines = rdd.count()
    unique_ips = rdd.map(lambda line: line.split(" ")[0]).distinct().count()
    return (total_lines, unique_ips)
```

### Step 5. 테스트 재실행 (통과 확인)

```bash
$ uv run pytest modules/m01_rdd_basics/ -k "test_count" -v

PASSED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_lines_and_ips_returns_tuple
PASSED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_lines_total
PASSED modules/m01_rdd_basics/test_problems.py::TestEasy::test_count_unique_ips
```

모든 테스트를 통과하면 다음 문제로 넘어갑니다.

---

## 풀이를 막혔을 때

`solutions.py`에 단계별 주석이 달린 참고 풀이가 있습니다. 먼저 스스로 시도해본 후 확인하세요.

```python
# modules/m01_rdd_basics/solutions.py
def count_lines_and_ips(sc, path: str) -> tuple[int, int]:
    # Step 1: 텍스트 파일을 RDD로 읽기
    rdd = sc.textFile(path)
    # Step 2: 전체 라인 수 (Action)
    total_lines = rdd.count()
    # Step 3: IP 추출 → 중복 제거 → 카운트
    unique_ips = rdd.map(lambda line: line.split(" ")[0]).distinct().count()
    return (total_lines, unique_ips)
```

---

## 데이터 소스

| 파일 | 경로 | 설명 |
|------|------|------|
| `access_logs.txt` | `data/logs/` | Apache CLF 형식 액세스 로그 30행 |
| `orders.csv` | `data/ecommerce/` | 이커머스 주문 데이터 30행 |
| `products.csv` | `data/ecommerce/` | 상품 카탈로그 10행 |
| `customers.json` | `data/ecommerce/` | 고객 정보 10건 (중첩 JSON) |
| `stock_prices.csv` | `data/finance/` | 주식 가격 데이터 30행 (AAPL/GOOGL/MSFT) |

M02, M04, M05 등 일부 모듈은 `src/helpers/data_generator.py`의 Faker 기반 함수로 대용량 데이터를 동적 생성합니다 (seed=42로 재현 가능).
