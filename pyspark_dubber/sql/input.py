from pathlib import Path

import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.types import StructType


class DataFrameReader:
    def parquet(self, *paths: str | Path) -> DataFrame:
        return DataFrame(ibis.read_parquet(paths))

    @incompatibility(
        "Most parameters are accepted but completely ignored.\n\n"
        "The path cannot be a RDD like in pyspark for now. "
        "Schema is only supported as a StructType, DLL strings are unsupported."
    )
    def json(
        self,
        path: str | Path | list[str | Path],
        schema: str | StructType | None = None,
        *args,
        **kwargs,
    ) -> DataFrame:
        # TODO: better jsonl support, ibis has very thin support for reading / writing it
        return DataFrame(ibis.read_json(path))
