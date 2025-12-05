import contextlib
import functools
import io
import sys
from collections.abc import Callable
from io import StringIO
from pathlib import Path
from typing import Generator, Any

import pytest
from pyspark.sql import SparkSession, DataFrame

from pyspark_dubber.replace_pyspark import replace_pyspark
from pyspark_dubber.sql import (
    SparkSession as DubberSparkSession,
    DataFrame as DubberDataFrame,
)

# Ensure pyspark_dubber is importable
ROOT_PATH = Path(__file__).parent.parent
sys.path.append(str(ROOT_PATH))


@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def spark_dubber() -> DubberSparkSession:
    return DubberSparkSession.builder.getOrCreate()


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


def assert_df_equal(dubber_df: DubberDataFrame, spark_df: DataFrame) -> None:
    assert dubber_df.collect() == spark_df.collect()


@contextlib.contextmanager
def capture_output() -> Generator[StringIO, Any, None]:
    prev_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield sys.stdout
    sys.stdout = prev_stdout


def comparison_test(func: Callable) -> Callable:
    """Rust code with pyspark and pyspark-dubber and asserts the results are equal."""

    @functools.wraps(func)
    def _test(**kwargs):
        spark = SparkSession.builder.getOrCreate()
        spark_dubber = SparkSession.builder.getOrCreate()
        kwargs.pop("spark")

        with capture_output() as pyspark_output:
            spark_result = func(spark=spark, **kwargs)

        with replace_pyspark(), capture_output() as dubber_output:
            dubber_result = func(spark=spark_dubber, **kwargs)

        spark_stdout = pyspark_output.getvalue()
        dubber_stdout = dubber_output.getvalue()

        if spark_result is not None:
            spark_result = spark_result.toPandas().to_dict(orient="records")
        if dubber_result is not None:
            dubber_result = dubber_result.toPandas().to_dict(orient="records")

        assert dubber_stdout == spark_stdout
        assert dubber_result == spark_result

        print(f"\npyspark:\n{spark_stdout}\npyspark-dubber\n{dubber_stdout}\n")

    return _test
