from datetime import date, datetime

from tests.conftest import comparison_test, parametrize


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
    functions = load("sql.functions")

    df = spark.createDataFrame([(datetime(2024, 2, 13, 22, 4, 17),)], ("date",))
    df.printSchema()

    df.select("*", functions.date_format("date", fmt)).show()
