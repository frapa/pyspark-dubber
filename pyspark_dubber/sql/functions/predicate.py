import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


@incompatibility(
    "This function does not raise an actual exception like PySpark's assert_true. "
    "Instead, it returns null when the condition is true and the error message when false. "
    "Users can check for non-null values to detect assertion failures."
)
def assert_true(col: ColumnOrName, errMsg: ColumnOrName | str | None = None) -> Expr:
    """Returns null if the input column is true; returns error message otherwise.

    Note: Unlike PySpark, this does not raise an exception. See incompatibility note.
    """
    col_expr = _col_fn(col)

    if errMsg is None:
        error_msg = ibis.literal(f"'{col_expr}' is not true!")
    elif isinstance(errMsg, str):
        error_msg = ibis.literal(errMsg)
    else:
        error_msg = _col_fn(errMsg).to_ibis()

    condition = col_expr.to_ibis()
    result = ibis.ifelse(condition, ibis.null(), error_msg)
    return Expr(result).alias(f"assert_true({col_expr})")


def isnull(col: ColumnOrName) -> Expr:
    col = _col_fn(col)
    return col.isNull().alias(f"({col} IS NULL)")


def isnotnull(col: ColumnOrName) -> Expr:
    col = _col_fn(col)
    return col.isNotNull().alias(f"({col} IS NOT NULL)")


def equal_null(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    col1 = _col_fn(col1)
    col2 = _col_fn(col2)
    return col1.eqNullSafe(col2).alias(f"equal_null({col1}, {col2})")


def isnan(col: ColumnOrName) -> Expr:
    col = _col_fn(col)
    result = col.to_ibis().isnan()
    # isnan returns false for null values (thanks spark!)
    return Expr(ibis.coalesce(result, ibis.literal(False))).alias(f"isnan({col})")
