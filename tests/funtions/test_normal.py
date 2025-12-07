from tests.conftest import parametrize, comparison_test


@parametrize(
    base={"func_name": "abs"},
    capitalized={"func_name": "ABS"},
)
@comparison_test
def test_call_function(spark, load, func_name: str) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [(1,), (-2,), (None,)],
        ["a"],
    )

    df.select("*", functions.call_function(func_name, "a")).show()


@parametrize(
    add={"sql": "a + 1"},
    func={"sql": "abs(a)"},
    boolean={"sql": "a > 0 AND !is_deleted"},
    case={"sql": "CASE WHEN a > 0 THEN 1 ELSE 0 END"},
    case_no_else={"sql": "CASE WHEN a > 0 THEN 1 END"},
)
@comparison_test
def test_expr(spark, load, sql: str) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (1, False),
            (2, True),
            (-2, True),
            (None, False),
        ],
        ["a", "is_deleted"],
    )

    df.printSchema()
    df.show()
    df.select("*", functions.expr(sql).alias(sql)).printSchema()
    df.select("*", functions.expr(sql).alias(sql)).show()
