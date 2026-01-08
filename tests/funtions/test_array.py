from tests.conftest import comparison_test


@comparison_test
def test_array_contains(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, [1, 2]), (2, [3, 4])],
        ("a", "b"),
    )

    df.select(
        "*",
        functions.array_contains("b", 1),
        functions.array_contains(df.b, df.a),
    ).show()


@comparison_test
def test_array_contains_string_literal(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(None,), (["a", "b", "c"],)],
        ["data"],
    )

    df.select(functions.array_contains(df.data, "a")).show()


@comparison_test
def test_array_append(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, [1, 2]), (2, [3, 4])],
        ("a", "b"),
    )

    df.select("*", functions.array_append("b", 5)).show()


@comparison_test
def test_array_append_string_literal(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(None,), (["a", "b", "c"],)],
        ["data"],
    )

    df.select(functions.array_append(df.data, "d")).show()


@comparison_test
def test_array_compact(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, None, 2],), ([None, None],), ([3, 4],)],
        ["data"],
    )

    df.select("*", functions.array_compact("data")).show()


@comparison_test
def test_array_max(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 5, 3],), ([10, 2, 8],)],
        ("arr",),
    )

    df.select("*", functions.array_max("arr")).show()


@comparison_test
def test_array_min(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 5, 3],), ([10, 2, 8],)],
        ("arr",),
    )

    df.select("*", functions.array_min("arr")).show()


@comparison_test
def test_array_position(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 2, 3],), ([4, 5, 6],)],
        ("arr",),
    )

    df.select(
        "*",
        functions.array_position("arr", 2),
        functions.array_position(df.arr, 5),
    ).show()


@comparison_test
def test_array_remove(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 2, 2, 3],), ([4, 5, 4, 6],)],
        ("arr",),
    )

    df.select(
        "*",
        functions.array_remove("arr", 2),
        functions.array_remove(df.arr, 4),
    ).show()


@comparison_test
def test_array_repeat(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1,), (2,)],
        ("a",),
    )

    df.select("*", functions.array_repeat("a", 3)).show()


@comparison_test
def test_size(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 2, 3],), ([4, 5],)],
        ("arr",),
    )

    df.select("*", functions.size("arr")).show()


@comparison_test
def test_sort_array(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([3, 1, 2],), ([6, 4, 5],)],
        ("arr",),
    )
    df.select("*", functions.sort_array("arr")).show()


@comparison_test
def test_flatten(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([[1, 2], [3, 4]],), ([[5], [6, 7, 8]],)],
        ("arr",),
    )
    df.select("*", functions.flatten("arr")).show()


@comparison_test
def test_array_function(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)],
        ("a", "b", "c"),
    )

    df.select("*", functions.array("a", "b", "c")).show()


@comparison_test
def test_array_join(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(["a", "b", "c"],), (["x", "y"],)],
        ("arr",),
    )

    df.select("*", functions.array_join("arr", ",")).show()


@comparison_test
def test_arrays_zip(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            {"int_prop": [1, 2, 3], "str_prop": ["a", "b", "c"]},
            {"int_prop": [4, 5], "str_prop": ["d", "e"]},
        ],
    )

    result = df.select(
        "*",
        functions.arrays_zip(
            df.int_prop,
            df.str_prop,
        ),
        functions.arrays_zip(
            functions.col("int_prop"),
            functions.col("str_prop"),
        ).alias("second"),
        functions.arrays_zip(
            functions.col("int_prop").alias("a"),
            functions.col("str_prop").alias("b"),
        ).alias("third"),
    )

    result.printSchema()
    result.show()

    return result
