import ibis

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import col as col_fn
from pyspark_dubber.sql.functions.array import _get_name
from pyspark_dubber.sql.functions.normal import ColumnOrName


def struct(*cols: ColumnOrName) -> Expr:
    ibis_cols = [col_fn(c).to_ibis() for c in cols]
    names = [_get_name(c) for c in ibis_cols]
    names_str = ", ".join(names)
    return Expr(ibis.struct(dict(zip(names, ibis_cols)))).alias(f"struct({names_str})")
