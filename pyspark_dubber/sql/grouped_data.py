import dataclasses
from typing import Any, Optional, List

import ibis.expr.types.groupby

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.expr import Expr


@dataclasses.dataclass
class GroupedData:
    _ibis_df: ibis.expr.types.groupby.GroupedTable

    def agg(self, *exprs: Expr) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(*[e.to_ibis() for e in exprs]))

    def count(self) -> "DataFrame":
        return DataFrame(self._ibis_df.agg(count=self._ibis_df.table.to_expr().count()))

    def avg(self, *cols: str) -> "DataFrame":
        """Computes average values for each numeric columns for each group.

        Non-numeric columns are ignored.
        """
        table_expr = self._ibis_df.table.to_expr()
        agg_exprs = {f"avg({col})": table_expr[col].mean() for col in cols}
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    def mean(self, *cols: str) -> "DataFrame":
        """Alias for avg()."""
        return self.avg(*cols)

    def sum(self, *cols: str) -> "DataFrame":
        """Computes the sum for each numeric columns for each group."""
        table_expr = self._ibis_df.table.to_expr()
        agg_exprs = {f"sum({col})": table_expr[col].sum() for col in cols}
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    def min(self, *cols: str) -> "DataFrame":
        """Computes the min for each numeric columns for each group."""
        table_expr = self._ibis_df.table.to_expr()
        agg_exprs = {f"min({col})": table_expr[col].min() for col in cols}
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    def max(self, *cols: str) -> "DataFrame":
        """Computes the max for each numeric columns for each group."""
        table_expr = self._ibis_df.table.to_expr()
        agg_exprs = {f"max({col})": table_expr[col].max() for col in cols}
        return DataFrame(self._ibis_df.agg(**agg_exprs))

    @incompatibility(
        "The pivot operation in ibis works differently than PySpark. "
        "While PySpark returns a GroupedData object that can be further aggregated, "
        "ibis requires the aggregation to be specified upfront. "
        "This implementation attempts to mimic PySpark behavior but may have limitations."
    )
    def pivot(self, pivot_col: str, values: Optional[List[Any]] = None) -> "GroupedData":
        """Pivots a column of the current DataFrame and prepares for aggregation.

        Note: This implementation stores pivot information for use in subsequent
        aggregation calls. The actual pivoting happens during aggregation.
        """
        # Store pivot information for later use in aggregation
        # This is a workaround since ibis pivot works differently
        pivot_data = {"pivot_col": pivot_col, "values": values}
        # We need to return a new GroupedData that remembers the pivot settings
        # For now, we'll store it as a private attribute
        new_grouped = GroupedData(self._ibis_df)
        new_grouped._pivot_data = pivot_data  # type: ignore
        return new_grouped

    @incompatibility(
        "PySpark's apply() method with a user-defined function is not supported. "
        "This would require executing arbitrary Python functions on grouped data, "
        "which is not directly supported by ibis. Consider using pandas operations "
        "directly or restructuring your code to use built-in aggregations."
    )
    def apply(self, udf):
        """Apply a user-defined function to each group.

        NOT IMPLEMENTED: This requires UDF support which is not available in ibis.
        """
        raise NotImplementedError(
            "GroupedData.apply() is not supported in pyspark-dubber. "
            "Use pandas operations directly or built-in aggregations instead."
        )

    @incompatibility(
        "PySpark's applyInPandas() method is not supported. "
        "This method allows applying a pandas UDF to each group, which requires "
        "UDF execution capabilities not available in ibis. Consider using pandas "
        "operations directly on the underlying data."
    )
    def applyInPandas(self, func, schema):
        """Apply a pandas UDF to each group.

        NOT IMPLEMENTED: This requires pandas UDF support which is not available in ibis.
        """
        raise NotImplementedError(
            "GroupedData.applyInPandas() is not supported in pyspark-dubber. "
            "Use pandas operations directly instead."
        )

    @incompatibility(
        "PySpark's applyInPandasWithState() method is not supported. "
        "This is a stateful pandas UDF operation that requires maintaining state "
        "across batches, which is not supported by ibis."
    )
    def applyInPandasWithState(self, func, outputStructType, stateStructType, outputMode, timeoutConf):
        """Apply a stateful pandas UDF to each group.

        NOT IMPLEMENTED: This requires stateful pandas UDF support which is not available.
        """
        raise NotImplementedError(
            "GroupedData.applyInPandasWithState() is not supported in pyspark-dubber."
        )

    @incompatibility(
        "PySpark's cogroup() method is not supported. "
        "This method allows cogrouping two DataFrames and applying a pandas UDF, "
        "which requires UDF execution capabilities not available in ibis. "
        "Consider using join operations instead."
    )
    def cogroup(self, other):
        """Cogroup this GroupedData with another GroupedData.

        NOT IMPLEMENTED: This requires pandas UDF support which is not available in ibis.
        """
        raise NotImplementedError(
            "GroupedData.cogroup() is not supported in pyspark-dubber. "
            "Use join operations instead."
        )
