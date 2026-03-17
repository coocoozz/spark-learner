"""
M07: UDF - 테스트
"""
import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from modules.m07_udf.problems import (
    normalize_by_category,
    register_address_parser_udf,
    register_normalize_udf,
    register_sentiment_udf,
)


@pytest.fixture
def text_df(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
    ])
    data = [
        ("1", "  Hello World  "),
        ("2", "SPARK  IS   GREAT"),
        ("3", "python programming"),
        ("4", None),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def address_df(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("address_raw", StringType(), True),
    ])
    data = [
        ("1", "123 Main Street 06100"),
        ("2", "서울시 강남구 06035"),
        ("3", "no zip code here"),
        ("4", None),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def review_df(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    data = [
        ("1", 5, "Electronics", 1200.0),
        ("2", 4, "Electronics", 450.0),
        ("3", 3, "Clothing", 85.5),
        ("4", 2, "Clothing", 120.0),
        ("5", 1, "Books", 29.99),
        ("6", 5, "Books", 45.0),
        ("7", None, "Food", 35.0),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestEasy:
    def test_normalize_udf_lowercase(self, spark, text_df):
        normalize = register_normalize_udf(spark)
        result = text_df.withColumn("normalized", normalize(col("text")))
        row = result.filter("id = '1'").first()
        assert row["normalized"] == "hello world"

    def test_normalize_udf_multiple_spaces(self, spark, text_df):
        normalize = register_normalize_udf(spark)
        result = text_df.withColumn("normalized", normalize(col("text")))
        row = result.filter("id = '2'").first()
        assert row["normalized"] == "spark is great"

    def test_normalize_udf_none(self, spark, text_df):
        normalize = register_normalize_udf(spark)
        result = text_df.withColumn("normalized", normalize(col("text")))
        row = result.filter("id = '4'").first()
        assert row["normalized"] is None


class TestMedium:
    def test_address_parser_udf_zip(self, spark, address_df):
        parser = register_address_parser_udf(spark)
        result = address_df.withColumn("parsed", parser(col("address_raw")))
        row = result.filter("id = '1'").first()
        assert row["parsed"]["zip_code"] == "06100"

    def test_address_parser_udf_no_zip(self, spark, address_df):
        parser = register_address_parser_udf(spark)
        result = address_df.withColumn("parsed", parser(col("address_raw")))
        row = result.filter("id = '3'").first()
        assert row["parsed"]["zip_code"] == "UNKNOWN"

    def test_sentiment_udf_positive(self, spark, review_df):
        sentiment = register_sentiment_udf(spark)
        result = review_df.withColumn("sentiment", sentiment(col("score")))
        row = result.filter("id = '1'").first()
        assert row["sentiment"] == "positive"

    def test_sentiment_udf_neutral(self, spark, review_df):
        sentiment = register_sentiment_udf(spark)
        result = review_df.withColumn("sentiment", sentiment(col("score")))
        row = result.filter("id = '3'").first()
        assert row["sentiment"] == "neutral"

    def test_sentiment_udf_negative(self, spark, review_df):
        sentiment = register_sentiment_udf(spark)
        result = review_df.withColumn("sentiment", sentiment(col("score")))
        row = result.filter("id = '4'").first()
        assert row["sentiment"] == "negative"

    def test_sentiment_udf_none(self, spark, review_df):
        sentiment = register_sentiment_udf(spark)
        result = review_df.withColumn("sentiment", sentiment(col("score")))
        row = result.filter("id = '7'").first()
        assert row["sentiment"] == "unknown"


class TestHard:
    def test_normalize_by_category_column_exists(self, review_df):
        result = normalize_by_category(review_df, "amount")
        assert "amount_zscore" in result.columns

    def test_normalize_by_category_mean_zero(self, review_df):
        result = normalize_by_category(review_df, "amount")
        from pyspark.sql.functions import avg, abs as spark_abs
        # 각 카테고리별 zscore 평균은 0에 가까워야 함
        electronics = result.filter("category = 'Electronics'")
        mean_zscore = electronics.agg(avg("amount_zscore")).first()[0]
        assert abs(mean_zscore) < 0.01
