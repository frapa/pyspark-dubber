from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


def asc_nulls_first(col: ColumnOrName) -> Expr:
    return _col_fn(col).asc_nulls_first()


def asc_nulls_last(col: ColumnOrName) -> Expr:
    return _col_fn(col).asc_nulls_last()


def asc(col: ColumnOrName) -> Expr:
    return _col_fn(col).asc()


def desc_nulls_first(col: ColumnOrName) -> Expr:
    return _col_fn(col).desc_nulls_first()


def desc_nulls_last(col: ColumnOrName) -> Expr:
    return _col_fn(col).desc_nulls_last()


def desc(col: ColumnOrName) -> Expr:
    return _col_fn(col).desc()
