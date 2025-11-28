from pathlib import Path

import ibis

from pyspark_dubber.sql.dataframe import DataFrame


class SparkInput:
    def parquet(self, *paths: str | Path) -> DataFrame:
        return DataFrame(ibis.read_parquet(paths))
