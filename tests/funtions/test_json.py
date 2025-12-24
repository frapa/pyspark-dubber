from tests.conftest import parametrize, comparison_test


@comparison_test
@parametrize(
    string={"data": [('{ "a": 1 }',), ("{}",)], "schema": "a int"}
)
def test_from_json(spark, load, data, schema) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(data, "data string")

    result = df.select("*", functions.from_json("data", schema))

    result.show()
    return result

