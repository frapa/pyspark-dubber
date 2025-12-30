import contextlib
import functools
import importlib
import io
import os
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

# On Windows it might not work otherwise
os.environ["PYSPARK_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def spark_dubber() -> DubberSparkSession:
    return DubberSparkSession.builder.getOrCreate()


# Placeholder injected by the @comparison_test decorator
@pytest.fixture
def load() -> Callable[[str], Any]: ...


def parametrize(**kwargs):
    if not kwargs:
        raise ValueError("No parametrization provided")

    first = next(iter(kwargs.values()))

    def _decorator(func: Callable) -> Callable:
        args = ",".join(first.keys())
        if len(first) == 1:
            arg = list(first.keys())[0]
            values = [case[arg] for case in kwargs.values()]
        else:
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


def load_object(prefix: str) -> Callable[[str], Any]:
    # Cache to avoid re-importing the same module over and over again
    @functools.cache
    def _load(path: str, prefix: str) -> Any:
        return importlib.import_module(f"{prefix}.{path}")

    return functools.partial(_load, prefix=prefix)


def comparison_test(func: Callable) -> Callable:
    """Runs code with pyspark and pyspark-dubber and asserts the results are equal."""

    @functools.wraps(func)
    def _test(**kwargs):
        spark = SparkSession.builder.getOrCreate()
        spark_dubber = DubberSparkSession.builder.getOrCreate()
        kwargs.pop("spark")
        kwargs.pop("load")

        with capture_output() as pyspark_output:
            spark_result = func(spark=spark, load=load_object("pyspark"), **kwargs)

        with capture_output() as dubber_output:
            dubber_result = func(
                spark=spark_dubber, load=load_object("pyspark_dubber"), **kwargs
            )

        spark_stdout = pyspark_output.getvalue()
        dubber_stdout = dubber_output.getvalue()

        if spark_result is not None:
            spark_result = spark_result.toPandas().to_dict(orient="records")
        if dubber_result is not None:
            dubber_result = dubber_result.toPandas().to_dict(orient="records")

        assert dubber_stdout == spark_stdout
        assert dubber_result == spark_result

        print(f"pyspark:\n{spark_stdout}\npyspark-dubber\n{dubber_stdout}\n")

    return _test
