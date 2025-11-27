import dataclasses

import ibis
import pandas

from pyspark_cl.sql.row import Row


@dataclasses.dataclass
class DataFrame:
    _ibis_df: ibis.Table

    def collect(self) -> list[Row]:
        return [Row(**d) for d in self._ibis_df.to_pandas().to_dict(orient="records")]

    def show(self) -> None:
        pass

    def toPandas(self) -> pandas.DataFrame:
        return self._ibis_df.to_pandas()
