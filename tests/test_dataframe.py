from pyspark_dubber.sql import SparkSession as DubberSparkSession
from tests.conftest import comparison_test

def test_dataframe_drop(spark_dubber: DubberSparkSession) -> None:
    """This uses the examples from the spark documentation"""
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


@comparison_test
def test_union_same_columns_same_order(spark, load):
    """Test basic union with same columns in same order."""
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(
        [(3, "c"), (4, "d")],
        ["id", "value"],
    )
    return df1.union(df2)


@comparison_test
def test_union_same_columns_different_order(spark, load):
    """Test union resolves by position, not by name.

    When columns have the same names but different order,
    union should combine by position (standard SQL behavior).
    This means df1's first column unions with df2's first column,
    regardless of column names.
    """
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    # Same column names but reversed order
    df2 = spark.createDataFrame(
        [("c", 3), ("d", 4)],
        ["value", "id"],
    )
    # Union by position: df1.id unions with df2.value, df1.value unions with df2.id
    # Result should have df1's column names with mixed data types
    return df1.union(df2)


@comparison_test
def test_union_different_column_names(spark, load):
    """Test union with completely different column names.

    Union resolves by position, so column names from first DataFrame are used.
    """
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["col_a", "col_b"],
    )
    df2 = spark.createDataFrame(
        [(3, "c"), (4, "d")],
        ["col_x", "col_y"],
    )
    # Result should use df1's column names: col_a, col_b
    return df1.union(df2)


@comparison_test
def test_union_all_same_as_union(spark, load):
    """Test that unionAll is equivalent to union (both keep duplicates)."""
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(
        [(1, "a"), (3, "c")],  # First row is duplicate
        ["id", "value"],
    )
    return df1.unionAll(df2)


@comparison_test
def test_union_with_distinct(spark, load):
    """Test union followed by distinct to get SQL UNION behavior."""
    df1 = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        ["id", "value"],
    )
    df2 = spark.createDataFrame(
        [(1, "a"), (3, "c")],  # First row is duplicate
        ["id", "value"],
    )
    return df1.union(df2).distinct().orderBy("id")