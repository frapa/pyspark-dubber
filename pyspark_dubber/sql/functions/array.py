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
def array_compact(col: ColumnOrName) -> Expr:
    return col.filter(lambda v: v.notnull())


@sql_func(col_name_args="col")
def array_distinct(col: ColumnOrName) -> Expr:
    return col.unique()


@sql_func(col_name_args=("col1", "col2"))
def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.intersect(col2)


@incompatibility("null_replacement is not natively in ibis")
@sql_func(col_name_args="col")
def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: str | None = None
) -> Expr:
    # TODO: Likely null_replacement can be implemented with a Map.
    return col.join(delimiter)


@sql_func(col_name_args="col")
def array_max(col: ColumnOrName) -> Expr:
    return col.maxs()


@sql_func(col_name_args="col")
def array_min(col: ColumnOrName) -> Expr:
    return col.mins()


@sql_func(col_name_args=("col", "value"))
def array_position(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    # Spark uses 1-based indexing, ibis uses 0-based
    return col.index(value) + 1


@sql_func(col_name_args=("col", "element"))
def array_remove(col: ColumnOrName, element: ColumnOrName | LiteralValue) -> Expr:
    return col.remove(element)


@sql_func(col_name_args="col")
def array_repeat(col: ColumnOrName, count: ColumnOrName | int) -> Expr:
    if isinstance(count, int):
        count_expr = count
    else:
        count_expr = col_fn(count).to_ibis()
    return ibis.array([col]).repeat(count_expr)


@sql_func(col_name_args="col")
def array_size(col: ColumnOrName) -> Expr:
    return col.length()


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


def array(*cols: ColumnOrName) -> Expr:
    ibis_cols = [col_fn(c).to_ibis() for c in cols]
    col_names = ", ".join(str(c) for c in cols)
    return Expr(ibis.array(ibis_cols)).alias(f"array({col_names})")


@sql_func(col_name_args=("col1", "col2"))
def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    # TODO: Potentially one could also express it in terms of other functions, thus abstracting this function from ibis completely.
    return col1.intersect(col2).length() > 0


def arrays_zip(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("arrays_zip requires at least one column")

    cols = [col_fn(c) for c in cols]
    ibis_cols = [c.to_ibis() for c in cols]
    result = (
        ibis_cols[0]
        .zip(*ibis_cols[1:])
        # Change names of the struct fields, from f1, f2 to the original column names
        .map(lambda s: ibis.struct({
            _get_name(c.to_ibis()): s[f]
            for f, c in zip(s.names, cols)
        }))
    )

    col_names = ", ".join(str(c) for c in cols)
    return Expr(result).alias(f"arrays_zip({col_names})")


def _get_name(col: ibis.Value | ibis.Deferred) -> str:
    """Gets alias or name of the column."""
    if isinstance(col, ibis.Value):
        return col.get_name()

    if (
        isinstance(col, ibis.Deferred)
        and isinstance(col._resolver, ibis.common.deferred.Call)
        and col._resolver.func.name.value in {"name", "alias"}
    ):
        return str(col._resolver.args[0].value)

    return str(col)