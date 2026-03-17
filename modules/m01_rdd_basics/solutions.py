"""
M01: RDD 기초 - 솔루션

각 문제의 정답 코드와 상세한 설명을 포함합니다.
학습자가 구현한 후 비교하거나, 막혔을 때 참고할 수 있습니다.
"""


def count_lines_and_ips(sc, path: str) -> tuple[int, int]:
    """
    풀이 설명:

    1단계: sc.textFile()로 로그 파일을 RDD로 읽습니다.
       - textFile은 HDFS, 로컬 파일 등을 지원합니다.
       - 각 줄이 RDD의 한 요소(element)가 됩니다.

    2단계: count()로 전체 라인 수를 구합니다.
       - count()는 RDD의 요소 수를 반환하는 'Action'입니다.
       - Action이 호출될 때 비로소 Spark 작업이 실행됩니다(Lazy Evaluation).

    3단계: map()으로 각 라인에서 IP를 추출합니다.
       - Apache CLF 형식에서 IP는 첫 번째 공백 전 문자열입니다.
       - split(" ")[0]으로 간단히 추출할 수 있습니다.
       - map()은 Transformation으로, 즉시 실행되지 않습니다.

    4단계: distinct()로 중복을 제거하고 count()로 셉니다.
       - distinct()는 셔플(Shuffle)을 발생시키는 wide transformation입니다.
       - 같은 IP가 여러 번 등장해도 한 번만 카운트됩니다.

    핵심 개념:
    - Transformation (지연 실행): map, filter, distinct, groupBy 등
    - Action (즉시 실행): count, collect, take, save 등
    - Lazy Evaluation: Action이 호출될 때까지 DAG만 구성, 실제 실행 안 함
    """
    rdd = sc.textFile(path)
    total_lines = rdd.count()
    unique_ips = rdd.map(lambda line: line.split(" ")[0]).distinct().count()
    return (total_lines, unique_ips)


def filter_by_status(sc, path: str, status_code: int) -> list[str]:
    """
    풀이 설명:

    1단계: 파일을 RDD로 읽습니다.

    2단계: filter()로 특정 상태 코드를 포함하는 라인만 통과시킵니다.
       - f" {status_code} " 패턴으로 검색하면 상태 코드가 독립된 토큰인지 확인할 수 있습니다.
       - 예: " 200 "은 "12001"이나 "2001"과 구분됩니다.
       - 더 정확한 방법은 line.split()[-2]로 끝에서 두 번째 필드를 확인하는 것입니다.

    3단계: collect()로 Python 리스트로 변환합니다.
       - 주의: collect()는 모든 데이터를 드라이버 메모리로 가져옵니다.
       - 대규모 데이터에서는 사용을 자제해야 합니다.
       - 학습/테스트 목적으로만 사용하세요.

    다른 방법:
        rdd.filter(lambda line: line.split()[-2] == str(status_code))
        → split()[-2]는 공백 기준 끝에서 두 번째 = 상태 코드
    """
    rdd = sc.textFile(path)
    filtered = rdd.filter(lambda line: f" {status_code} " in line)
    return filtered.collect()


def top_n_ips(sc, path: str, n: int) -> list[tuple[str, int]]:
    """
    풀이 설명:

    1단계: 각 라인에서 IP를 추출해 (ip, 1) 쌍을 만듭니다.
       - 이것이 "Pair RDD"의 기본 패턴입니다.
       - (키, 값) 형태의 RDD를 Pair RDD라고 합니다.

    2단계: reduceByKey()로 같은 IP의 카운트를 합산합니다.
       - reduceByKey는 같은 키끼리 값을 reduce(합산/집계)합니다.
       - groupByKey보다 효율적입니다 (맵 사이드 집계 수행).

    3단계: sortBy()로 횟수 내림차순 정렬합니다.
       - lambda x: x[1] → 튜플의 두 번째 요소(횟수)로 정렬
       - ascending=False → 내림차순

    4단계: take(n)으로 상위 n개만 가져옵니다.
       - take(n)은 collect()와 달리 n개만 드라이버로 가져옵니다.

    성능 팁:
    - top(n, key=lambda x: x[1])을 사용하면 전체 정렬 없이 상위 n개를 효율적으로 가져올 수 있습니다.
    """
    rdd = sc.textFile(path)
    ip_counts = (
        rdd
        .map(lambda line: (line.split(" ")[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )
    return ip_counts.take(n)


def requests_by_hour(sc, path: str) -> list[tuple[int, int]]:
    """
    풀이 설명:

    1단계: 타임스탬프에서 시간(hour)을 추출합니다.
       로그 형식: ... [10/Oct/2023:13:55:36 -0700] ...
       파싱 방법:
         - split("[")[1]: "[" 이후 문자열 → "10/Oct/2023:13:55:36 -0700] ..."
         - split(":")[1]: 첫 번째 ":" 이후 → 시간 부분 "13"
         - int()로 변환

    2단계: (hour, 1) Pair RDD 생성 → reduceByKey 합산

    3단계: sortBy(lambda x: x[0])로 시간 오름차순 정렬

    4단계: collect()로 전체 결과 반환

    다른 파싱 방법:
        import re
        match = re.search(r'\\[(\\d{2}/\\w+/\\d{4}):(\\d{2}):', line)
        hour = int(match.group(2))
    """
    rdd = sc.textFile(path)

    def extract_hour(line: str) -> int:
        # "[10/Oct/2023:13:55:36 -0700]" 에서 시간 추출
        timestamp_part = line.split("[")[1]
        hour = int(timestamp_part.split(":")[1])
        return hour

    result = (
        rdd
        .map(lambda line: (extract_hour(line), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[0])
    )
    return result.collect()


def bytes_by_status(sc, path: str) -> list[tuple[int, int]]:
    """
    풀이 설명:

    로그의 마지막 두 필드가 상태 코드와 바이트 수입니다:
        ... "GET /index.html HTTP/1.1" 200 2326
                                       ^^^  ^^^^
                                    [-2]   [-1]

    1단계: 각 라인을 파싱해 (status_code, bytes) 쌍을 생성합니다.
       - line.split()[-2]: 상태 코드 문자열 → int 변환
       - line.split()[-1]: 바이트 수 문자열 → int 변환 (단, "-"이면 0)

    2단계: reduceByKey로 상태 코드별 바이트 합산

    3단계: sortBy(lambda x: x[0])로 상태 코드 오름차순 정렬

    주의: 파싱 실패 라인은 filter로 제거하거나 try-except로 처리합니다.

    핵심 개념 - Wide vs Narrow Transformation:
    - Narrow: map, filter (파티션 내에서만 처리, 셔플 없음)
    - Wide: reduceByKey, groupByKey, sortBy (셔플 발생, 파티션 간 데이터 이동)
    """
    rdd = sc.textFile(path)

    def parse_line(line: str):
        parts = line.split()
        if len(parts) < 2:
            return None
        status = int(parts[-2])
        byte_str = parts[-1]
        bytes_sent = 0 if byte_str == "-" else int(byte_str)
        return (status, bytes_sent)

    result = (
        rdd
        .map(parse_line)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[0])
    )
    return result.collect()
