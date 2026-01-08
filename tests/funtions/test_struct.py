from tests.conftest import comparison_test


@comparison_test
def test_struct(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))

    df.select("*", functions.struct("age", df.name)).show()
