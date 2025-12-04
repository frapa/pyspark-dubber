from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions import col as col_fn
from pyspark_dubber.sql.functions.normal import ColumnOrName


def ascii(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().ascii_str())
