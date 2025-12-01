from pyspark_dubber.sql import SparkSession as DubberSparkSession


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
