import functools
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark_cl.sql import SparkSession as CLSparkSession, DataFrame as CLDataFrame

# Ensure pyspark_cl is importable
ROOT_PATH = Path(__file__).parent.parent
sys.path.append(str(ROOT_PATH))


@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def spark_cl() -> CLSparkSession:
    return CLSparkSession.builder.getOrCreate()


def parametrize(**kwargs):
    if not kwargs:
        raise ValueError("No parametrization provided")

    first = next(iter(kwargs.values()))

    def _decorator(func: Callable) -> Callable:
        args = ", ".join(first.keys())
        values = [[case[arg] for arg in first.keys()] for case in kwargs.values()]
        ids = list(kwargs.keys())
        return pytest.mark.parametrize(args, values, ids=ids)(func)

    return _decorator


def assert_df_equal(cl_df: CLDataFrame, spark_df: DataFrame) -> None:
    assert cl_df.collect() == spark_df.collect()
