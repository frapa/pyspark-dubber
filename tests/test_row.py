from pyspark.sql import Row
from pyspark_cl.sql import Row as CLRow


def test_row_eq():
    assert Row(a=1, b="ciao") == CLRow(a=1, b="ciao")
    assert CLRow(a=1, b="ciao") == Row(a=1, b="ciao")


def test_row_eq_list():
    assert [Row(a=1, b="ciao")] == [CLRow(a=1, b="ciao")]
