from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName
from pyspark_dubber.sql.functions.array import array_size

def size(col: ColumnOrName) -> Expr:
  return array_size(col).alias(f"size({col})")

@sql_func(col_name_args=("col"))
def element_at(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
  if isinstance(index, int):
    # Convert 1-based to 0-based index
    # Spark: positive indices are 1-based, negative indices work from end (-1 is last)
    if index > 0:
      idx = index - 1
    else:
      idx = index
  else:
    idx = col - 1
  return Expr(col[idx]).alias(f"element_at({col}, {index})")


@incompatibility("comparator parameter is not supported")
@sql_func(col_name_args="col")
def array_sort(col: ColumnOrName, comparator=None) -> Expr:
  return col.sort()