import ibis

from pyspark_dubber.sql.expr import Expr, lit

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


