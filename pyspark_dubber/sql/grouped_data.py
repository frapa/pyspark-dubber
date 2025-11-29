import dataclasses

import ibis.expr.types.groupby

from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.expr import Expr


@dataclasses.dataclass
class GroupedData:
    _ibis_df: ibis.expr.types.groupby.GroupedTable

    def agg(self, *exprs: Expr) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(*[e.to_ibis() for e in exprs]))

    def count(self) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(count=self._ibis_df.table.to_expr().count()))
