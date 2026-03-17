"""
M01: RDD 기초 - 테스트

학습자가 problems.py를 구현하면 이 테스트를 통과해야 합니다.
"""
import os

import pytest

from modules.m01_rdd_basics.problems import (
    bytes_by_status,
    count_lines_and_ips,
    filter_by_status,
    requests_by_hour,
    top_n_ips,
)

DATA_PATH = os.path.join(os.path.dirname(__file__), "../../data/logs/access_logs.txt")


class TestEasy:
    def test_count_lines_and_ips_returns_tuple(self, sc):
        result = count_lines_and_ips(sc, DATA_PATH)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_count_lines_total(self, sc):
        total_lines, _ = count_lines_and_ips(sc, DATA_PATH)
        assert total_lines == 30

    def test_count_unique_ips(self, sc):
        _, unique_ips = count_lines_and_ips(sc, DATA_PATH)
        assert unique_ips == 9

    def test_filter_by_status_200(self, sc):
        result = filter_by_status(sc, DATA_PATH, 200)
        assert isinstance(result, list)
        assert len(result) > 0
        for line in result:
            assert " 200 " in line

    def test_filter_by_status_404(self, sc):
        result = filter_by_status(sc, DATA_PATH, 404)
        assert len(result) == 2
        for line in result:
            assert " 404 " in line

    def test_filter_by_status_500(self, sc):
        result = filter_by_status(sc, DATA_PATH, 500)
        assert len(result) == 2
        for line in result:
            assert " 500 " in line


class TestMedium:
    def test_top_n_ips_returns_list(self, sc):
        result = top_n_ips(sc, DATA_PATH, 3)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_top_n_ips_sorted_descending(self, sc):
        result = top_n_ips(sc, DATA_PATH, 5)
        counts = [count for _, count in result]
        assert counts == sorted(counts, reverse=True)

    def test_top_n_ips_top1(self, sc):
        result = top_n_ips(sc, DATA_PATH, 1)
        ip, count = result[0]
        # 192.168.1.1이 가장 많은 요청을 보냄 (7회)
        assert count >= 5

    def test_requests_by_hour_returns_list(self, sc):
        result = requests_by_hour(sc, DATA_PATH)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_requests_by_hour_sorted(self, sc):
        result = requests_by_hour(sc, DATA_PATH)
        hours = [h for h, _ in result]
        assert hours == sorted(hours)

    def test_requests_by_hour_all_counts_positive(self, sc):
        result = requests_by_hour(sc, DATA_PATH)
        for hour, count in result:
            assert 0 <= hour <= 23
            assert count > 0


class TestHard:
    def test_bytes_by_status_returns_list(self, sc):
        result = bytes_by_status(sc, DATA_PATH)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_bytes_by_status_sorted(self, sc):
        result = bytes_by_status(sc, DATA_PATH)
        statuses = [s for s, _ in result]
        assert statuses == sorted(statuses)

    def test_bytes_by_status_200_is_largest(self, sc):
        result = bytes_by_status(sc, DATA_PATH)
        status_dict = dict(result)
        assert 200 in status_dict
        assert status_dict[200] > status_dict.get(404, 0)

    def test_bytes_by_status_all_non_negative(self, sc):
        result = bytes_by_status(sc, DATA_PATH)
        for _, total_bytes in result:
            assert total_bytes >= 0
