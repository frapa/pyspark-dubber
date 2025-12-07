from pyspark.sql import Row

from pyspark_dubber.sql import Row as DubberRow


def test_row_eq():
    assert Row(a=1, b="ciao") == DubberRow(a=1, b="ciao")
    assert DubberRow(a=1, b="ciao") == Row(a=1, b="ciao")


def test_row_eq_list():
    assert [Row(a=1, b="ciao")] == [DubberRow(a=1, b="ciao")]
