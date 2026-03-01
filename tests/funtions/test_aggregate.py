import pytest

from tests.conftest import comparison_test, parametrize


@parametrize(
    count={"func": lambda f: f.count("*")},
    avg={"func": lambda f: f.avg("num")},
    bit_and={"func": lambda f: f.bit_and("num")},
    bool_and={"func": lambda f: f.bool_and("bool")},
    every={"func": lambda f: f.every("bool")},
    some={"func": lambda f: f.some("bool")},
    collect_list={"func": lambda f: f.collect_list("num")},
    collect_set={"func": lambda f: f.sort_array(f.collect_set("num")).alias("collect_set(num)")},
    first={"func": lambda f: f.first("num")},
    last={"func": lambda f: f.last("num")},
    min={"func": lambda f: f.min("num")},
    max={"func": lambda f: f.max("num")},
    # max_by={"func": lambda f: f.max_by("num", "other")},
    median={"func": lambda f: f.median("num")},
    mode={"func": lambda f: f.mode("num")},
    stddev={"func": lambda f: f.stddev("num")},
    variance={"func": lambda f: f.variance("num")},
    kurtosis={"func": lambda f: f.kurtosis("num")},
    sum={"func": lambda f: f.sum("num")},
    # product={"func": lambda f: f.product("num")},
)
@comparison_test
def test_agg(spark, load, func) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            ("a", 1, 5, True),
            ("a", 1, 4, False),
            ("b", 0, 5, True),
            ("b", 3, None, True),
            ("b", None, 4, True),
        ],
        ["group", "num", "other", "bool"],
    )

    df.groupby("group").agg(func(functions)).orderBy("group").show()


@pytest.mark.xfail(reason="ibis quantile does not support array of percentiles")
@comparison_test
def test_agg_percentile(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            ("a", 1, 5, True),
            ("a", 1, 4, False),
            ("b", 0, 5, True),
            ("b", 3, None, True),
            ("b", None, 4, True),
        ],
        ["group", "num", "other", "bool"],
    )

    df.groupby("group").agg(functions.percentile("num", [0.25, 0.75])).orderBy("group").show()


@pytest.mark.xfail(reason="DuckDB approx_quantile returns DOUBLE instead of input column type")
@comparison_test
def test_agg_approx_percentile(spark, load) -> None:
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            ("a", 1, 5, True),
            ("a", 1, 4, False),
            ("b", 0, 5, True),
            ("b", 3, None, True),
            ("b", None, 4, True),
        ],
        ["group", "num", "other", "bool"],
    )

    df.groupby("group").agg(functions.approx_percentile("num", 0.25)).orderBy("group").show()
