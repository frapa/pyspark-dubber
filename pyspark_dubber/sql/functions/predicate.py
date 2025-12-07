import ibis

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, _col_fn


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
