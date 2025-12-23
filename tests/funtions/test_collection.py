from tests.conftest import comparison_test


@comparison_test
def test_array_size(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 2, 3],), ([4, 5],)],
        ("arr",),
    )
    df.select(
        "*",
        functions.array_size("arr"),
    ).show()


@comparison_test
def test_element_at(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([1, 2, 3],), ([4, 5, 6],)],
        ("arr",),
    )
    df.select(
        "*",
        functions.element_at("arr", 1),
        functions.element_at(df.arr, 2),
        functions.element_at("arr", -1),
    ).show()


@comparison_test
def test_array_sort(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [([3, 1, 2],), ([6, 4, 5],)],
        ("arr",),
    )
    df.select(
        "*",
        functions.array_sort("arr"),
    ).show()
