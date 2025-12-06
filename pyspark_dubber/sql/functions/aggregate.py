import ibis

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


def count(col: ColumnOrName) -> Expr:
    if col == "*":
        return Expr(ibis.deferred.count())
    return Expr(_col_fn(col).to_ibis().count())
