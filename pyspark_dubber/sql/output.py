import dataclasses
from pathlib import Path
from typing import Literal

import ibis


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
    def csv(self, path: str) -> None:
        path = Path(path) / "part-00000.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._ibis_df.to_csv(path)
