import pytest
from pyspark.sql.session import SparkSession
from pyspark_cl.sql.session import SparkSession as CLSparkSession
from pyspark_cl.sql.types import StructType, StringType, StructField
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
    data, spark: SparkSession, spark_cl: CLSparkSession
) -> None:
    with pytest.raises(Exception) as err:
        spark.createDataFrame(data)

    with pytest.raises(Exception) as err_cl:
        spark_cl.createDataFrame(data)

    assert (
        type(err_cl.value).__name__ == type(err.value).__name__
    ), f"'{err_cl.value}' != '{err.value}'"
    assert str(err_cl.value) == str(err.value)


@parametrize(
    mismatch=dict(
        data=[1, 2, 3],
        schema=StructType([StructField("seq", StringType(), True)]),
    )
)
def test_session_createDataFrame_validate_schema_error(
    data, schema, spark: SparkSession, spark_cl: CLSparkSession
) -> None:
    with pytest.raises(Exception) as err:
        spark.createDataFrame(data, schema=schema.to_pyspark())

    with pytest.raises(Exception) as err_cl:
        spark_cl.createDataFrame(data, schema=schema)

    assert (
        type(err_cl.value).__name__ == type(err.value).__name__
    ), f"{err_cl.value} != {err.value}"
    assert str(err_cl.value) == str(err.value)


@parametrize(
    infer=dict(
        data=[("Alice", 1)],
        schema=None,
    ),
)
def test_session_createDataFrame(
    data, schema, spark: SparkSession, spark_cl: CLSparkSession
) -> None:
    assert_df_equal(
        spark_cl.createDataFrame(data, schema),
        spark.createDataFrame(data, schema),
    )
