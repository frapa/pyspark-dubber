from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


def isnull(col: ColumnOrName) -> Expr:
    return _col_fn(col).isNull()


def isnotnull(col: ColumnOrName) -> Expr:
    return _col_fn(col).isNotNull()


def equal_null(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return _col_fn(col1).eqNullSafe(col2)


def isnan(col: ColumnOrName) -> Expr:
    return Expr(_col_fn(col).to_ibis().isnan())
