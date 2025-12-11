from pyspark_dubber import __version__
from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn, Column


def broadcast(df: "DataFrame") -> "DataFrame":
    # Does nothing as ibis does not support broadcasting,
    # as most SQL engines do not have such a concept and aren't distributed.
    return df


def version() -> Expr:
    return lit(__version__)


def bitwise_not(col: ColumnOrName) -> Expr:
    return ~_col_fn(col)


bitwiseNOT = bitwise_not


@incompatibility(
    "This function does not raise an actual exception like PySpark's assert_true. "
    "Instead, it returns null when the condition is true and the error message when false. "
    "Users can check for non-null values to detect assertion failures."
)
def assert_true(col: ColumnOrName, errMsg: Column | str | None = None) -> Expr:
    """Returns null if the input column is true; returns error message otherwise.

    Note: Unlike PySpark, this does not raise an exception. See incompatibility note.
    """
    col_expr = _col_fn(col)

    if errMsg is None:
        error_msg = ibis.literal(f"'{col_expr}' is not true!")
    else:
        error_msg = ibis.lit(errMsg)

    condition = col_expr.to_ibis()
    result = ibis.ifelse(condition, ibis.null(), error_msg)
    return Expr(result).alias(f"assert_true({col_expr})")
