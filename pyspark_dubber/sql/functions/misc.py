from pyspark_dubber import __version__
from pyspark_dubber.sql.expr import Expr, lit


def version() -> Expr:
    return lit(__version__)
