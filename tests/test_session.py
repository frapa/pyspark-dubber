import pytest
from pyspark.sql.session import SparkSession

from pyspark_dubber.sql.session import SparkSession as DubberSparkSession
from pyspark_dubber.sql.types import StructType, StringType, StructField
from tests.conftest import assert_df_equal, parametrize


@pytest.mark.parametrize(
    "data",
    [
        [],
        [1, 2, 3],
        [(None, 1)],
    ],
    ids=["empty", "bare_list", "nulls"],
)
def test_session_createDataFrame_infer_error(
    data, spark: SparkSession, spark_dubber: DubberSparkSession
) -> None:
    with pytest.raises(Exception) as pyspark_err:
        spark.createDataFrame(data)

    with pytest.raises(Exception) as dubber_err:
        spark_dubber.createDataFrame(data)

    assert (
        type(dubber_err.value).__name__ == type(pyspark_err.value).__name__
    ), f"'{dubber_err.value}' != '{pyspark_err.value}'"
    assert str(dubber_err.value) == str(pyspark_err.value)


@parametrize(
    mismatch=dict(
        data=[1, 2, 3],
        schema=StructType([StructField("seq", StringType(), True)]),
    )
)
def test_session_createDataFrame_validate_schema_error(
    data, schema, spark: SparkSession, spark_dubber: DubberSparkSession
) -> None:
    with pytest.raises(Exception) as pyspark_err:
        spark.createDataFrame(data, schema=schema.to_pyspark())

    with pytest.raises(Exception) as dubber_err:
        spark_dubber.createDataFrame(data, schema=schema)

    assert (
        type(dubber_err.value).__name__ == type(pyspark_err.value).__name__
    ), f"{dubber_err.value} != {pyspark_err.value}"
    assert str(dubber_err.value) == str(pyspark_err.value)


@parametrize(
    infer=dict(
        data=[("Alice", 1)],
        schema=None,
    ),
)
def test_session_createDataFrame(
    data, schema, spark: SparkSession, spark_dubber: DubberSparkSession
) -> None:
    assert_df_equal(
        spark_dubber.createDataFrame(data, schema),
        spark.createDataFrame(data, schema),
    )
