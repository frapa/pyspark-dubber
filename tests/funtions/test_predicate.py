from tests.conftest import comparison_test


@comparison_test
def test_isnull(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, None), (None, 2)],
        ("a", "b"),
    )
    df.select(
        "*",
        functions.isnull("a"),
        functions.isnull(df.b),
    ).show()


@comparison_test
def test_isnotnull(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, None), (None, 2)],
        ("a", "b"),
    )
    df.select(
        "*",
        functions.isnotnull("a"),
        functions.isnotnull(df.b),
    ).show()


@comparison_test
def test_isnan(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(float("nan"),), (1.0,), (None,)],
        ["a"],
    )
    df.select(
        "*",
        functions.isnan("a"),
        functions.isnan(df.a).alias("a_is_nan_col"),
    ).show()


@comparison_test
def test_equal_null(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1, 1), (None, None), (None, 2), (2, None), (2, 2)],
        ["x", "y"],
    )

    df.select(
        "*",
        functions.equal_null("x", df.y),
        functions.equal_null(df["x"], functions.lit(2)),
    ).show()
