import ibis

from pyspark_dubber.sql import DataFrame
from pyspark_dubber.sql.expr import Expr, LiteralValue
from pyspark_dubber.sql.functions.normal import ColumnOrName

def array_append(self,
    col: ColumnOrName,
    value: LiteralValue | Expr
) -> "DataFrame":
  if isinstance(col, str):
    col_expr = self[col]
  else:
    col_name = col.get_name()
    col_expr = col
  if isinstance(value, LiteralValue):
    value = ibis.literal(value)
  new_col = col_expr.concat(ibis.array([value]))
  return self.mutate(**{col_name: new_col})