import pytest

from pyspark_dubber.sql import SparkSession as DubberSparkSession
from tests.conftest import comparison_test, parametrize


def test_dataframe_drop(spark_dubber: DubberSparkSession) -> None:
    df = spark_dubber.createDataFrame(
        [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"]
    )
    df2 = spark_dubber.createDataFrame([(80, "Tom"), (85, "Bob")], ["height", "name"])

    result = df.drop("age").toPandas().to_dict(orient="records")
    assert result == [
        {"name": "Tom"},
        {"name": "Alice"},
        {"name": "Bob"},
    ]

    result = df.drop(df.age).toPandas().to_dict(orient="records")
    assert result == [
        {"name": "Tom"},
        {"name": "Alice"},
        {"name": "Bob"},
    ]

    result = df.join(df2, df.name == df2.name).drop("name").sort("age")
    assert result.toPandas().to_dict(orient="records") == [
        {"age": 14, "height": 80},
        {"age": 16, "height": 85},
    ]

    df3 = df.join(df2)
    result = df3.drop("name", "name_right").sort("age", "height")
    assert result.toPandas().to_dict(orient="records") == [
        {"age": 14, "height": 80},
        {"age": 14, "height": 85},
        {"age": 16, "height": 80},
        {"age": 16, "height": 85},
        {"age": 23, "height": 80},
        {"age": 23, "height": 85},
    ]


@parametrize(
    same_order={
        "data": [(3, "c"), (4, "d")],
        "schema": ["id", "value"],
    },
    different_column_names={
        "data": [(3, "c"), (4, "d")],
        "schema": ["col_x", "col_y"],
    },
)
@comparison_test
def test_union(spark, load, data, schema):
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(data, schema)

    df1.show()
    df2.show()

    df1.union(df2).show()


@comparison_test
def test_union_error(spark, load):
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df2 = spark.createDataFrame([("c", 3), ("d", 4)], ["value", "id"])

    df1.show()
    df2.show()

    with pytest.raises(Exception):
        df1.union(df2).show()


@comparison_test
def test_union_by_name(spark, load) -> None:
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(
        [("c", 3), ("d", 4)],
        ["value", "id"],
    )

    df1.show()
    df2.show()

    df1.unionByName(df2).show()


@comparison_test
def test_union_by_name_error(spark, load) -> None:
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(
        [(3, "c"), (4, "d")],
        ["col_x", "col_y"],
    )

    df1.show()
    df2.show()

    errors = load("errors")
    with pytest.raises(errors.AnalysisException):
        df1.unionByName(df2).show()
