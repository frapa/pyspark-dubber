from tests.conftest import parametrize, comparison_test


@parametrize(
    simple_object={"data": [('{ "a": 1 }',), ("{}",)], "schema": "a int"},
    simple_list={"data": [("[1, 2, 3]",), ("[null, 1]",)], "schema": "array<int>"},
    complex_object={
        "data": [('{"a": null, "b": 2, "c": true, "d": { "g": ["g", "gg"] }}',)],
        "schema": "struct<a int, b string, c boolean, d struct<f string, g array<string>>>",
    },
    duckdb_casing={        "data": [('{"caseSensitive": 123}',)],        "schema": "caseSensitive INT"    },
)
@comparison_test
def test_from_json(spark, load, data, schema) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(data, "data string")

    result = df.select("*", functions.from_json("data", schema))

    result.printSchema()
    result.show()
    # return result
