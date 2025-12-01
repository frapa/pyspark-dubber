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
