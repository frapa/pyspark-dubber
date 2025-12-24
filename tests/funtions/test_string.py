from tests.conftest import comparison_test, parametrize


@comparison_test
def test_trim_and_btrim(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            ("   Spark SQL   ",),
            ("no-trim",),
            ("   both   ",),
        ],
        ["s"],
    )

    df.select(
        "*",
        functions.trim("s").alias("trim"),
        functions.btrim("s").alias("btrim"),
    ).show()

    df.select(
        "*",
        # Only from version 4.0.0
        functions.trim("s", functions.lit(" SLmb")).alias("trim"),
        functions.btrim("s", functions.lit("SLmb")).alias("btrim"),
    ).show()


@comparison_test
def test_base64(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("Spark SQL",)], ["s"])

    df.select("*", functions.base64("s")).show()


@comparison_test
def test_unbase64(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("Spark SQL",)], ["s"])

    df.select("*", functions.unbase64("s")).show()


# The function was added in Spark 4.0.0
@comparison_test
def test_randstr(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])

    df.select(functions.length(functions.randstr(16)).alias("len")).show()


@comparison_test
def test_split_part(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [("11.12.13", ".", 3)],
        ["a", "b", "c"],
    )

    # Positive
    df.select("*", functions.split_part("a", "b", "c")).show()
    # Negative
    df.select("*", functions.split_part(df.a, df.b, functions.lit(-2))).show()


@comparison_test
def test_substring_index(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("a.b.c.d",)], ["s"])

    # Positive
    df.select("*", functions.substring_index(df.s, ".", 2)).show()
    # Negative
    # df.select("*", functions.substring_index("s", ".", -3)).show()


@parametrize(
    hex={"fmt": "hex"},
    utf8={"fmt": "utf-8"},
    utf8_variant={"fmt": "utf8"},
    base64={"fmt": "base64"},
)
@comparison_test
def test_to_binary(spark, load, fmt) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("abc",)], ["e"])

    result = df.select(functions.to_binary(df.e, functions.lit(fmt)))
    result.printSchema()
    result.show()


@parametrize(
    hex={"fmt": "hex"},
    utf8={"fmt": "utf-8"},
    utf8_variant={"fmt": "utf8"},
    base64={"fmt": "base64"},
)
@comparison_test
def test_printf(spark, load, fmt) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame([("abc",)], ["e"])

    result = df.select(functions.to_binary(df.e, functions.lit(fmt)))
    result.printSchema()
    result.show()
