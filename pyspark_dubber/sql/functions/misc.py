from pyspark_dubber import __version__
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


def broadcast(df: "DataFrame") -> "DataFrame":
    # Does nothing as ibis does not support broadcasting,
    # as most SQL engines do not have such a concept and aren't distributed.
    return df


def version() -> Expr:
    return lit(__version__)


def bitwise_not(col: ColumnOrName) -> Expr:
    return ~_col_fn(col)


bitwiseNOT = bitwise_not
