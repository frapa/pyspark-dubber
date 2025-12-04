from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName


def to_timestamp(col: ColumnOrName, format: str | None = None) -> Expr:
    return Expr()


def current_timestamp() -> Expr:
    return Expr()
