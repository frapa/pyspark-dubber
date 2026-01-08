import inspect
from collections.abc import Callable

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn
from pyspark_dubber.sql.functions.array import array_size

UnaryOrBinary = Callable[[Expr], Expr] | Callable[[Expr, Expr], Expr]


def size(col: ColumnOrName) -> Expr:
    return array_size(col).alias(f"size({col})")


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


def get(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
    return element_at(col, index).alias(f"get({col}, {index})")


@incompatibility("comparator parameter is not supported")
def array_sort(col: ColumnOrName, comparator=None) -> Expr:
    col_expr = col_fn(col).to_ibis()
    result = col_expr.sort()
    # PySpark displays the internal lambda function as the column name
    # We replicate this for compatibility, though it's not user-friendly
    ugly_name = f"array_sort({col}, lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable()))"
    return Expr(result).alias(ugly_name)


@sql_func(col_name_args="col")
def filter(col: ColumnOrName, f: UnaryOrBinary) -> Expr:
    if len(inspect.signature(f).parameters) == 1:
        ibis_func = lambda v: f(Expr(v)).to_ibis()
    else:
        ibis_func = lambda v, i: f(Expr(v), Expr(i)).to_ibis()

    return col.filter(ibis_func)


@sql_func(col_name_args="col")
def transform(col: ColumnOrName, f: UnaryOrBinary) -> Expr:
    if len(inspect.signature(f).parameters) == 1:
        ibis_func = lambda v: f(Expr(v)).to_ibis()
    else:
        ibis_func = lambda v, i: f(Expr(v), Expr(i)).to_ibis()

    return col.map(ibis_func)
