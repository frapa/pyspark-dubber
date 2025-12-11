import ibis

from pyspark_dubber import __version__
from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn


def broadcast(df: "DataFrame") -> "DataFrame":
    # Does nothing as ibis does not support broadcasting,
    # as most SQL engines do not have such a concept and aren't distributed.
    return df


def version() -> Expr:
    return lit(__version__)


def bitwise_not(col: ColumnOrName) -> Expr:
    return ~col_fn(col)


bitwiseNOT = bitwise_not


@sql_func(col_name_args="col")
def assert_true(col: ColumnOrName, errMsg: Expr | str | None = None) -> Expr:
    @ibis.udf.scalar.python
    def _assert_true(condition: bool, error_msg: str) -> None:
        if not condition:
            raise RuntimeError(error_msg)

    return _assert_true(col, lit(errMsg).to_ibis())
