from pyspark_dubber import __version__
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions.base import ColumnOrName, _col_fn


def version() -> Expr:
    return lit(__version__)


def bitwise_not(col: ColumnOrName) -> Expr:
    return ~_col_fn(col)


bitwiseNOT = bitwise_not
