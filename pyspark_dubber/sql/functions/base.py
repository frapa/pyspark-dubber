import ibis

from pyspark_dubber.sql.expr import Expr, lit, WhenExpr

ColumnOrName = Expr | str


def _col_fn(col: str) -> Expr:
    return Expr(ibis.deferred[col])


col = _col_fn
column = _col_fn
lit = lit


def count(col: ColumnOrName) -> Expr:
    if col == "*":
        return Expr(ibis.deferred.count())
    return Expr(_col_fn(col).to_ibis().count())


def asc_nulls_first(col: ColumnOrName) -> Expr:
    return Expr(ibis.asc(_col_fn(col).to_ibis(), nulls_first=True))


def asc_nulls_last(col: ColumnOrName) -> Expr:
    return Expr(ibis.asc(_col_fn(col).to_ibis(), nulls_first=False))


asc = asc_nulls_first


def desc_nulls_first(col: ColumnOrName) -> Expr:
    return Expr(ibis.desc(_col_fn(col).to_ibis(), nulls_first=True))


def desc_nulls_last(col: ColumnOrName) -> Expr:
    return Expr(ibis.desc(_col_fn(col).to_ibis(), nulls_first=False))


desc = desc_nulls_first


def isnull(col: ColumnOrName) -> Expr:
    return Expr(_col_fn(col).to_ibis().isnull())


def isnotnull(col: ColumnOrName) -> Expr:
    return Expr(_col_fn(col).to_ibis().notnull())


def when(condition: Expr, value: Expr) -> Expr:
    return WhenExpr(None, [(condition, value)])
