import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, LiteralValue
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn
from pyspark_dubber.sql.expr import lit


@sql_func(col_name_args=("col"))
def array_append(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    return col.concat(ibis.array([lit(value).to_ibis()]))


@sql_func(col_name_args=("col"))
def array_contains(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    return col.contains(lit(value).to_ibis())


@sql_func(col_name_args="col")
def array_distinct(col: ColumnOrName) -> Expr:
    return col.unique()


@sql_func(col_name_args=("col1", "col2"))
def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.intersect(col2)


@incompatibility("null_replacement is not natively in ibis")
def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: str | None = None
) -> Expr:
    return Expr(col_fn(col).to_ibis().join(delimiter)).alias(
        f"array_join({col}, {delimiter})"
    )


@sql_func(col_name_args="col")
def array_max(col: ColumnOrName) -> Expr:
    return col.maxs()


@sql_func(col_name_args="col")
def array_min(col: ColumnOrName) -> Expr:
    return col.mins()


@sql_func(col_name_args=("col", "value"))
def array_position(col: ColumnOrName, value: ColumnOrName | LiteralValue) -> Expr:
    # Spark uses 1-based indexing, ibis uses 0-based
    return col.index(value) + 1


@sql_func(col_name_args=("col", "element"))
def array_remove(col: ColumnOrName, element: ColumnOrName | LiteralValue) -> Expr:
    return col.remove(element)


def array_repeat(col: ColumnOrName, count: ColumnOrName | int) -> Expr:
    col_expr = col_fn(col).to_ibis()
    if isinstance(count, int):
        count_expr = count
    else:
        count_expr = col_fn(count).to_ibis()
    return Expr(ibis.array([col_expr]).repeat(count_expr)).alias(
        f"array_repeat({col}, {count})"
    )


@sql_func(col_name_args="col")
def array_size(col: ColumnOrName) -> Expr:
    return col.length()


def size(col: ColumnOrName) -> Expr:
    return array_size(col).alias(f"size({col})")


def array_sort(col: ColumnOrName) -> Expr:
    col_expr = col_fn(col).to_ibis()
    result = col_expr.sort()
    # PySpark displays the internal lambda function as the column name
    # We replicate this for compatibility, though it's not user-friendly
    ugly_name = f"array_sort({col}, lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable()))"
    return Expr(result).alias(ugly_name)


@incompatibility(
    "Descending sort (asc=False) is not supported. Arrays are always sorted in ascending order."
)
def sort_array(col: ColumnOrName, asc: bool = True) -> Expr:
    col_expr = col_fn(col).to_ibis()
    result = col_expr.sort()
    return Expr(result).alias(f"sort_array({col}, {str(asc).lower()})")


@sql_func(col_name_args=("col1", "col2"))
def array_union(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.union(col2)


@sql_func(col_name_args="col")
def flatten(col: ColumnOrName) -> Expr:
    return col.flatten()


def array_concat(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("concat requires at least one column")
    result = col_fn(cols[0]).to_ibis()
    for c in cols[1:]:
        result = result.concat(col_fn(c).to_ibis())
    col_names = ", ".join(str(c) for c in cols)
    return Expr(result).alias(f"concat({col_names})")


def array(*cols: ColumnOrName) -> Expr:
    ibis_cols = [col_fn(c).to_ibis() for c in cols]
    col_names = ", ".join(str(c) for c in cols)
    return Expr(ibis.array(ibis_cols)).alias(f"array({col_names})")


def element_at(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
    col_expr = col_fn(col).to_ibis()
    if isinstance(index, int):
        # Convert 1-based to 0-based index
        # Spark: positive indices are 1-based, negative indices work from end (-1 is last)
        if index > 0:
            idx = index - 1
        else:
            idx = index
    else:
        idx = col_fn(index).to_ibis() - 1
    return Expr(col_expr[idx]).alias(f"element_at({col}, {index})")


@sql_func(col_name_args=("col1", "col2"))
def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.intersect(col2).length() > 0


def arrays_zip(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("arrays_zip requires at least one column")
    ibis_cols = [col_fn(c).to_ibis() for c in cols]
    result = ibis_cols[0].zip(*ibis_cols[1:])
    col_names = ", ".join(str(c) for c in cols)
    return Expr(result).alias(f"arrays_zip({col_names})")
