import dataclasses
from pathlib import Path
from typing import Literal

import ibis

from pyspark_dubber.docs import incompatibility


@dataclasses.dataclass
class SparkOutput:
    _ibis_df: ibis.Table

    def mode(
        self,
        saveMode: (
            Literal["append", "overwrite", "error", "errorifexists", "ignore"] | None
        ),
    ) -> "SparkOutput":
        return self

    def option(self, key: str, value: bool | int | float | str | None) -> "SparkOutput":
        return self

    # TODO: other parameters
    @incompatibility(
        "Most parameters are unsupported, the writing of files cannot be reproduced 1:1"
        "because it depends on spark internals such as partitions."
    )
    def csv(self, path: str) -> None:
        path = Path(path) / "part-00000.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._ibis_df.to_csv(path)

    @incompatibility(
        "Most parameters are unsupported, the writing of files cannot be reproduced 1:1"
        "because it depends on spark internals such as partitions."
    )
    def parquet(self, path: str) -> None:
        path = Path(path) / "part-00000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._ibis_df.to_parquet(path)
