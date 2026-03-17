"""
M03: DataFrame 기초 - 테스트
"""
import os

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from modules.m03_dataframe_basics.problems import (
    create_from_dicts,
    flatten_nested,
    load_and_describe,
    load_with_schema,
    safe_union,
)

ORDERS_PATH = os.path.join(os.path.dirname(__file__), "../../data/ecommerce/orders.csv")
CUSTOMERS_PATH = os.path.join(os.path.dirname(__file__), "../../data/ecommerce/customers.json")


class TestEasy:
    def test_load_and_describe_returns_tuple(self, spark):
        result = load_and_describe(spark, ORDERS_PATH)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_load_and_describe_row_count(self, spark):
        df, row_count, col_count = load_and_describe(spark, ORDERS_PATH)
        assert row_count == 30

    def test_load_and_describe_col_count(self, spark):
        df, row_count, col_count = load_and_describe(spark, ORDERS_PATH)
        assert col_count == 9  # orders.csv has 9 columns

    def test_load_and_describe_is_dataframe(self, spark):
        from pyspark.sql import DataFrame
        df, _, _ = load_and_describe(spark, ORDERS_PATH)
        assert isinstance(df, DataFrame)

    def test_load_with_schema(self, spark):
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("age", LongType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("zip", StringType(), True),
            ]), True),
        ])
        df = load_with_schema(spark, CUSTOMERS_PATH, schema)
        assert df.count() == 10
        assert "customer_id" in df.columns

    def test_load_with_schema_types(self, spark):
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("age", LongType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("zip", StringType(), True),
            ]), True),
        ])
        df = load_with_schema(spark, CUSTOMERS_PATH, schema)
        assert df.schema["age"].dataType == LongType()


class TestMedium:
    def test_create_from_dicts(self, spark):
        data = [
            {"order_id": "O001", "amount": 100.0, "category": "Electronics"},
            {"order_id": "O002", "amount": 50.0, "category": "Clothing"},
        ]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True),
        ])
        df = create_from_dicts(spark, data, schema)
        assert df.count() == 2
        assert df.schema["amount"].dataType == DoubleType()

    def test_safe_union(self, spark):
        schema1 = StructType([
            StructField("order_id", StringType(), True),
            StructField("amount", StringType(), True),  # String type
        ])
        schema2 = StructType([
            StructField("order_id", StringType(), True),
            StructField("amount", DoubleType(), True),  # Double type
        ])
        df1 = spark.createDataFrame([("O001", "100.0")], schema=schema1)
        df2 = spark.createDataFrame([("O002", 200.0)], schema=schema2)
        result = safe_union(df1, df2)
        assert result.count() == 2
        assert result.schema["amount"].dataType == DoubleType()


class TestHard:
    def test_flatten_nested(self, spark):
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("zip", StringType(), True),
            ]), True),
        ])
        data = [("C001", "Alice", ("123 Main St", "12345"))]
        df = spark.createDataFrame(data, schema=schema)
        result = flatten_nested(df)
        assert "address" not in result.columns
        assert "address_street" in result.columns
        assert "address_zip" in result.columns
        assert result.count() == 1

    def test_flatten_nested_values(self, spark):
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("zip", StringType(), True),
            ]), True),
        ])
        data = [("C001", ("123 Main St", "12345"))]
        df = spark.createDataFrame(data, schema=schema)
        result = flatten_nested(df)
        row = result.first()
        assert row["address_street"] == "123 Main St"
        assert row["address_zip"] == "12345"
