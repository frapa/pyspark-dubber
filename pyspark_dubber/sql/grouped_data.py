import dataclasses

import ibis.expr.types.groupby

from pyspark_dubber.sql.dataframe import DataFrame


@dataclasses.dataclass
class GroupedData:
    _ibis_df: ibis.expr.types.groupby.GroupedTable

    def count(self) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(count=self._ibis_df.table.to_expr().count()))
