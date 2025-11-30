import inspect
import shutil
from pathlib import Path
from textwrap import indent

import numpy

from pyspark.sql import SparkSession, DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.group import GroupedData

from pyspark_dubber.sql import (
    DataFrame as DubberDataFrame,
    SparkSession as DubberSparkSession,
)
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
    api_reference()


API_AREAS = [
    (
        "SparkSession",
        SparkSession,
        DubberSparkSession,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.{api}.html",
    ),
    (
        "SparkSession.builder",
        SparkSession.Builder,
        DubberSparkSession.Builder,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.{api}.html",
    ),
    (
        "Input Formats",
        DataFrameReader,
        SparkInput,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.{api}.html",
    ),
    (
        "Output Formats",
        DataFrameWriter,
        SparkOutput,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.{api}.html",
    ),
    (
        "DataFrame",
        DataFrame,
        DubberDataFrame,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.{api}.html",
    ),
    (
        "GroupBy",
        GroupedData,
        DubberGroupedData,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.{api}.html",
    ),
    (
        "Functions",
        functions,
        dubber_functions,
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.{api}.html",
    ),
]


def api_coverage() -> None:
    counts = numpy.zeros(2, dtype=int)
    with capture_output() as compat_stdout:
        for name, spark_obj, dubber_obj, like_template in API_AREAS:
            counts += compare_objects(name, spark_obj, dubber_obj, like_template)

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
        note = getattr(
            getattr(dubber_obj, api, None), "__incompatibility_docs__", ""
        ).split("\n\n")[0]
        if note:
            url = f"/pyspark-dubber/API Reference/{obj_name}/{obj_name}.{api}"
        else:
            url = link_template.format(api=api)
        print(f"| [`{obj_name}.{api}`]({url}) | {mark} | {note} |")

    print()
    return len(spark_apis), len(dubber_apis)


def api_reference() -> None:
    for _, _, obj, link_template in API_AREAS:
        for api in _iter_apis(obj):
            api_func = getattr(obj, api)
            note = getattr(api_func, "__incompatibility_docs__", "")
            if note:
                obj_name = obj.__name__
                doc_path = Path(
                    ROOT / "docs" / "API Reference" / obj_name / f"{obj_name}.{api}.md"
                )
                doc_path.parent.mkdir(parents=True, exist_ok=True)

                url = link_template.format(api=api)
                sig = inspect.signature(api_func)
                args = "\n".join(
                    f"\t{s}," for arg, s in sig.parameters.items() if arg != "self"
                )

                doc_path.write_text(
                    f"# {obj_name}.{api}"
                    "\n\n"
                    "```python\n"
                    f"{obj_name}.{api}(\n"
                    f"{args}\n"
                    ")\n"
                    "```\n\n"
                    f"[PySpark API Reference]({url})"
                    "\n\n"
                    '!!! warning "Incompatibility Note"\n\n'
                    f"{indent(note, prefix='    ')}\n\n"
                    f"{api_func.__doc__ or ''}"
                )


def _iter_apis(obj) -> Iterable[str]:
    for api in dir(obj):
        api_func = getattr(obj, api)

        if api.startswith("_") or not callable(api_func):
            continue

        if obj is functions and getattr(obj, api).__module__ != "pyspark.sql.functions":
            continue

        yield api


if __name__ == "__main__":
    main()
