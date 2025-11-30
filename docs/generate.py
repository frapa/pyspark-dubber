import shutil
from pathlib import Path

import numpy

from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.group import GroupedData

from pyspark_dubber.sql import DataFrame as DubberDataFrame
from pyspark_dubber.sql import functions as dubber_functions
from pyspark_dubber.sql.grouped_data import GroupedData as DubberGroupedData
from pyspark_dubber.sql.input import SparkInput
from pyspark_dubber.sql.output import SparkOutput

from tests.test_pyspark_scripts import capture_output

ROOT = Path(__file__).parent.parent

from typing import Iterable


def main() -> None:
    shutil.copy(ROOT / "README.md", ROOT / "docs" / "index.md")
    api_coverage()


def api_coverage() -> None:
    counts = numpy.zeros(2, dtype=int)
    with capture_output() as compat_stdout:
        counts += compare_objects(
            "Input Formats",
            DataFrameReader,
            SparkInput,
            "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.{api}.html",
        )
        counts += compare_objects(
            "Output Formats",
            DataFrameWriter,
            SparkOutput,
            "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.{api}.html",
        )
        counts += compare_objects(
            "DataFrame",
            DataFrame,
            DubberDataFrame,
            "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.{api}.html",
        )
        counts += compare_objects(
            "GroupBy",
            GroupedData,
            DubberGroupedData,
            "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.{api}.html",
        )
        counts += compare_objects(
            "Functions",
            functions,
            dubber_functions,
            "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.{api}.html",
        )

    spark_count, dubber_count = counts
    total_coverage = dubber_count / spark_count * 100

    compat_page_body = compat_stdout.getvalue().replace("✓", ":material-check:")
    Path(ROOT / "docs" / "api_coverage.md").write_text(
        "# API Coverage\n\n"
        "This page shows which APIs are currently re-implemented by `pyspark-dubber`. "
        "This list is not exhaustive, showing mostly public functions and DataFrame APIs, "
        "however some additional APIs and magic methods are also implemented. "
        "\n\n"
        f"The overall approximate API coverage (with the caveats above) is {total_coverage:.1f} %."
        "\n\n"
        f"{compat_page_body}"
    )


def compare_objects(name, spark_obj, dubber_obj, link_template: str) -> tuple[int, int]:
    spark_apis = sorted(_iter_apis(spark_obj))
    dubber_apis = set(_iter_apis(dubber_obj))

    coverage = len(dubber_apis) / len(spark_apis) * 100
    print(f"## {name} ({coverage:.0f} %)\n")
    print(f"| API | Implemented | Notes |")
    print(f"| --- | :---------: | ----- |")

    obj_name = spark_obj.__name__
    for api in spark_apis:
        mark = ":material-check:" if api in dubber_apis else " "
        url = link_template.format(api=api)
        note = getattr(getattr(dubber_obj, api, None), "__incompatibility_docs__", "")
        print(f"| [`{obj_name}.{api}`]({url}) | {mark} | {note} |")

    print()
    return len(spark_apis), len(dubber_apis)


def _iter_apis(obj) -> Iterable[str]:
    for api in dir(obj):
        if api.startswith("_"):
            continue
        yield api


if __name__ == "__main__":
    main()
