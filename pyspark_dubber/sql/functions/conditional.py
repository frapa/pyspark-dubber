import ibis

from pyspark_dubber.sql.expr import Expr, LiteralValue
from pyspark_dubber.sql.expr import WhenExpr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn, lit
from pyspark_dubber.sql.functions.predicate import isnan, isnull


def coalesce(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("At least one column must be provided to coalesce()")
    return Expr(ibis.coalesce(_col_fn(c).to_ibis() for c in cols))


def ifnull(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return coalesce(col1, col2)


nvl = ifnull


def nvl2(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Expr:
    return when(isnull(col1), col3).otherwise(col2)


def nullif(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return Expr(ibis.nullif(_col_fn(col1).to_ibis() == _col_fn(col2).to_ibis()))


def zeroifnull(col: ColumnOrName) -> Expr:
    return coalesce(col, lit(0))


def nanvl(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return when(isnan(col1), lit(col2)).otherwise(col1)


def when(condition: Expr, value: Expr | LiteralValue) -> WhenExpr:
    return WhenExpr(None, [(condition, lit(value))])
