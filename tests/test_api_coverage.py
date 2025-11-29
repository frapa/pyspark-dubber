from typing import Iterable, Any

import numpy
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.group import GroupedData

from pyspark_dubber.sql import DataFrame as DubberDataFrame
from pyspark_dubber.sql import functions as dubber_functions
from pyspark_dubber.sql.grouped_data import GroupedData as DubberGroupedData
from pyspark_dubber.sql.input import SparkInput
from pyspark_dubber.sql.output import SparkOutput


def test_api_coverage() -> None:
    """This is not a test, but rather a script
    to calculate which percentage of the pyspark API
    is covered by pyspark-dubber.
    """
    print()

    counts = numpy.zeros(2, dtype=int)
    counts += _compare_objects("DataFrame", DataFrame, DubberDataFrame)
    counts += _compare_objects("Read Formats", DataFrameReader, SparkInput)
    counts += _compare_objects("Write Formats", DataFrameWriter, SparkOutput)
    counts += _compare_objects("GroupBy", GroupedData, DubberGroupedData)
    counts += _compare_objects("Functions", functions, dubber_functions)

    spark_count, dubber_count = counts
    total_coverage = dubber_count / spark_count * 100
    print(f"Total coverage: {total_coverage:.1f} %")


def _compare_objects(name: str, spark_obj, dubber_obj) -> tuple[int, int]:
    spark_apis = sorted(_iter_apis(spark_obj))
    dubber_apis = set(_iter_apis(dubber_obj))

    coverage = len(dubber_apis) / len(spark_apis) * 100
    print(f"{name} ({coverage:.1f} %)\n{'':=<50}")

    obj_name = spark_obj.__name__
    for api in spark_apis:
        mark = "✓" if api in dubber_apis else ""
        print(f"{obj_name}.{api:<32}{mark}")

    print()
    return len(spark_apis), len(dubber_apis)


def _iter_apis(obj) -> Iterable[str]:
    for api in dir(obj):
        if api.startswith("_"):
            continue
        yield api
