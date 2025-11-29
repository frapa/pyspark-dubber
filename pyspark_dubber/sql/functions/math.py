from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.base import ColumnOrName, col as col_fn


def avg(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().mean())


mean = avg
