from datetime import date, datetime, timezone

import pyspark
import pytest

from tests.conftest import comparison_test, parametrize

PYSPARK_4 = int(pyspark.__version__.split(".")[0]) >= 4
_LOCAL_UTC_OFFSET = datetime.now(timezone.utc).astimezone().utcoffset().total_seconds()
IS_UTC = _LOCAL_UTC_OFFSET == 0


@comparison_test
def test_add_months(spark, load):
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (date(2024, 2, 13), 1),
            (date(2025, 12, 8), 4),
        ],
        ("date", "months"),
    )
    df.printSchema()

    df.select(
        "*",
        functions.add_months("date", 2),
        functions.add_months("date", "months"),
    ).show()


@comparison_test
def test_date_diff(spark, load):
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (date(2015, 4, 8), date(2015, 5, 10)),
        ],
        ("d1", "d2"),
    )
    df.printSchema()

    df.select(
        "*",
        functions.date_diff("d2", "d1"),
        functions.date_diff("d1", "d2"),
    ).show()


@comparison_test
def test_date_from_unix_date(spark, load):
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (0,),
            (365,),
            (18262,),
        ],
        ("days",),
    )
    df.printSchema()

    df.select(
        "*",
        functions.date_from_unix_date(df.days),
    ).show()


@pytest.mark.skipif(not PYSPARK_4, reason="dayname requires PySpark 4+")
@comparison_test
def test_dayname_dayofweek(spark, load):
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (date(2024, 2, 13),),
            (date(2025, 12, 8),),
        ],
        ("date",),
    )
    df.printSchema()

    df.select(
        "*",
        functions.dayname("date"),
        functions.dayofweek("date"),
    ).show()


@pytest.mark.skipif(not PYSPARK_4, reason="monthname requires PySpark 4+")
@comparison_test
def test_monthname(spark, load):
    functions = load("sql.functions")

    df = spark.createDataFrame(
        [
            (date(2024, 2, 13),),
            (date(2025, 12, 8),),
        ],
        ("date",),
    )
    df.printSchema()

    df.select(
        "*",
        functions.monthname("date"),
    ).show()


@parametrize(
    yyyy={"fmt": "yyyy"},
    yyy={"fmt": "yyy"},
    yy={"fmt": "yy"},
    y={"fmt": "y"},
    DDD={"fmt": "DDD"},
    DD={"fmt": "DD"},
    D={"fmt": "D"},
    dd={"fmt": "dd"},
    d={"fmt": "d"},
    L={"fmt": "L"},
    MMMM={"fmt": "MMMM"},
    MMM={"fmt": "MMM"},
    MM={"fmt": "MM"},
    M={"fmt": "M"},
    EEEE={"fmt": "EEEE"},
    EEE={"fmt": "EEE"},
    EE={"fmt": "EE"},
    E={"fmt": "E"},
    F={"fmt": "F"},
    HH={"fmt": "HH"},
    H={"fmt": "H"},
    mm={"fmt": "mm"},
    m={"fmt": "m"},
    ss={"fmt": "ss"},
    s={"fmt": "s"},
    hh={"fmt": "hh"},
    h={"fmt": "h"},
    kk={"fmt": "kk"},
    k={"fmt": "k"},
    X={"fmt": "X"},
    Z={"fmt": "Z"},
    x={"fmt": "x"},
    z={"fmt": "z"},
    a={"fmt": "a"},
)
@comparison_test
def test_date_format(spark, load, fmt: str):
    if fmt in ("X", "Z", "x", "z"):
        pytest.xfail("DuckDB does not track timezone info for timestamps")
    if fmt == "F":
        pytest.xfail("DuckDB strftime %w does not match PySpark's F (day of week in month)")

    functions = load("sql.functions")

    df = spark.createDataFrame([(datetime(2024, 2, 13, 22, 4, 17),)], ("date",))
    df.printSchema()

    df.select("*", functions.date_format("date", fmt)).show()
