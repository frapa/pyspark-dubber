import dataclasses

import ibis.expr.types
import ibis.expr.types.groupby

from pyspark_dubber.sql.functions import count
from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.expr import Expr


@dataclasses.dataclass
class GroupedData:
    _ibis_df: ibis.expr.types.groupby.GroupedTable

    @property
    def _table(self) -> ibis.Table:
        return self._ibis_df.table.to_expr()

    def agg(self, *exprs: Expr) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(*[e.to_ibis() for e in exprs]))

    def count(self) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(count=count().to_ibis()))

    def avg(self, *cols: str) -> "DataFrame":
        agg_exprs = {
            f"avg({col})": self._table[col].mean()
            for col in cols
            if self._table[col].type().is_numeric()
        }
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    mean = avg

    def sum(self, *cols: str) -> "DataFrame":
        agg_exprs = {
            f"sum({col})": self._table[col].sum()
            for col in cols
            if self._table[col].type().is_numeric()
        }
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    def min(self, *cols: str) -> "DataFrame":
        agg_exprs = {
            f"min({col})": self._table[col].min()
            for col in cols
            if self._table[col].type().is_numeric()
        }
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    def max(self, *cols: str) -> "DataFrame":
        agg_exprs = {
            f"max({col})": self._table[col].max()
            for col in cols
            if self._table[col].type().is_numeric()
        }
        return DataFrame(self._ibis_df.agg(**agg_exprs))
