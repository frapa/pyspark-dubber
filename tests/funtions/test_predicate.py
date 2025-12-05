from tests.conftest import comparison_test


@comparison_test
def test_isnull(spark) -> None:
    from pyspark.sql.functions import isnull

    df = spark.createDataFrame(
        [(1, None), (None, 2)],
        ("a", "b"),
    )
    df.select(
        "*",
        isnull("a"),
        isnull(df.b),
    ).show()


@comparison_test
def test_isnotnull(spark) -> None:
    from pyspark.sql.functions import isnotnull

    df = spark.createDataFrame(
        [(1, None), (None, 2)],
        ("a", "b"),
    )
    df.select(
        "*",
        isnotnull("a").alias("a_not_null"),
        isnotnull(df.b).alias("b_not_null"),
    ).show()


@comparison_test
def test_isnan(spark) -> None:
    from pyspark.sql.functions import isnan

    df = spark.createDataFrame(
        [(float("nan"),), (1.0,), (None,)],
        ["a"],
    )
    df.select(
        "*",
        isnan("a").alias("a_is_nan"),
        isnan(df.a).alias("a_is_nan_col"),
    ).show()


@comparison_test
def test_equal_null(spark) -> None:
    from pyspark.sql.functions import equal_null, lit

    df = spark.createDataFrame(
        [(1, 1), (None, None), (None, 2), (2, None), (2, 2)],
        ["x", "y"],
    )

    df.select(
        "*",
        equal_null("x", df.y).alias("x_eqnull_y"),
        equal_null(df["x"], lit(2)).alias("x_eqnull_2"),
    ).show()
