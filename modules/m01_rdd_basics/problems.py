"""
M01: RDD 기초
도메인: 서버 액세스 로그
데이터: data/logs/access_logs.txt (Apache CLF 형식)
"""


def count_lines_and_ips(sc, path: str) -> tuple[int, int]:
    """
    [Easy] 총 라인 수와 고유 IP 수 반환

    Apache CLF(Common Log Format) 형식의 로그 파일을 읽어
    전체 라인 수와 고유 IP 주소의 수를 반환합니다.

    로그 형식 예시:
        192.168.1.1 - frank [10/Oct/2023:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326
        ^^^^^^^^^^ 첫 번째 공백 전이 IP 주소입니다.

    Args:
        sc: SparkContext 인스턴스
        path: 로그 파일 경로

    Returns:
        (total_lines, unique_ip_count) 튜플
        예: (30, 9)

    Hint:
        - sc.textFile(path): 파일의 각 줄을 RDD 요소로 읽기
        - rdd.count(): 전체 요소 수 반환 (Action)
        - rdd.map(lambda line: ...): 각 요소 변환 (Transformation)
        - line.split(" ")[0]: 공백으로 분리해 첫 번째 토큰(IP) 추출
        - rdd.distinct(): 중복 제거 (Transformation)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def filter_by_status(sc, path: str, status_code: int) -> list[str]:
    """
    [Easy] 특정 HTTP 상태 코드의 로그 라인 필터링

    로그 파일에서 지정한 HTTP 상태 코드(200, 404, 500 등)에 해당하는
    라인만 골라 리스트로 반환합니다.

    로그 형식에서 상태 코드 위치:
        192.168.1.1 - frank [...] "GET /index.html HTTP/1.1" 200 2326
                                                              ^^^
        공백으로 분리하면 인덱스 8 또는 끝에서 두 번째 위치입니다.

    Args:
        sc: SparkContext 인스턴스
        path: 로그 파일 경로
        status_code: 필터링할 HTTP 상태 코드 (예: 200, 404, 500)

    Returns:
        해당 상태 코드를 포함하는 로그 라인 리스트

    Hint:
        - rdd.filter(lambda line: ...): 조건에 맞는 요소만 통과
        - str(status_code): 정수를 문자열로 변환
        - f" {status_code} " 또는 line.split()[-2]: 상태 코드 확인 방법
        - rdd.collect(): RDD를 Python 리스트로 반환 (Action)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def top_n_ips(sc, path: str, n: int) -> list[tuple[str, int]]:
    """
    [Medium] IP별 요청 횟수 상위 N개 반환

    각 IP 주소가 몇 번 요청을 보냈는지 집계하고,
    요청 횟수가 많은 순서로 상위 N개를 반환합니다.

    Args:
        sc: SparkContext 인스턴스
        path: 로그 파일 경로
        n: 반환할 상위 개수

    Returns:
        [(ip, count), ...] 형태의 리스트, 횟수 내림차순 정렬
        예: [("192.168.1.1", 5), ("10.0.0.5", 4), ...]

    Hint:
        - map으로 (ip, 1) 쌍 생성
        - reduceByKey(lambda a, b: a + b): 키별 합산
        - sortBy(lambda x: x[1], ascending=False): 값 기준 내림차순 정렬
        - take(n): 상위 n개 반환 (Action)
    """
    # TODO: 구현하세요
    raise NotImplementedError


def requests_by_hour(sc, path: str) -> list[tuple[int, int]]:
    """
    [Medium] 시간대별 요청 수 집계

    로그의 타임스탬프에서 시간(hour)을 추출하여
    각 시간대별 요청 수를 집계합니다.

    타임스탬프 형식: [10/Oct/2023:13:55:36 -0700]
    대괄호 안의 콜론(:) 두 번째 이후가 시:분:초입니다.

    Args:
        sc: SparkContext 인스턴스
        path: 로그 파일 경로

    Returns:
        [(hour, count), ...] 형태의 리스트, 시간 오름차순 정렬
        예: [(13, 2), (14, 8), (15, 6), ...]

    Hint:
        - 타임스탬프 파싱: line.split("[")[1].split(":")[1] 로 시간 추출
        - map으로 (hour, 1) 쌍 생성
        - reduceByKey로 합산
        - sortBy(lambda x: x[0]): 키(시간) 기준 정렬
        - collect(): 전체 결과 반환
    """
    # TODO: 구현하세요
    raise NotImplementedError


def bytes_by_status(sc, path: str) -> list[tuple[int, int]]:
    """
    [Hard] 로그 파싱 후 상태코드별 총 바이트 수 집계

    각 로그 라인을 파싱하여 HTTP 상태 코드별로
    전송된 총 바이트 수를 합산합니다.

    로그 형식:
        IP - user [timestamp] "method path protocol" status bytes
        마지막 두 필드가 상태코드와 바이트 수입니다.
        바이트가 "-"인 경우 0으로 처리합니다.

    Args:
        sc: SparkContext 인스턴스
        path: 로그 파일 경로

    Returns:
        [(status_code, total_bytes), ...] 리스트, 상태코드 오름차순
        예: [(200, 52847), (201, 512), (401, 128), (403, 128), (404, 512), (500, 512)]

    Hint:
        - line.split()[-2]: 상태 코드 (끝에서 두 번째)
        - line.split()[-1]: 바이트 수 (마지막)
        - 바이트가 "-"이면 0, 아니면 int() 변환
        - map으로 (status_int, bytes_int) 쌍 생성
        - reduceByKey로 합산 후 sortBy로 정렬
    """
    # TODO: 구현하세요
    raise NotImplementedError
