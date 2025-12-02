import ibis

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.base import lit
from pyspark_dubber.sql.functions.base import ColumnOrName, col as col_fn


def avg(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().mean())


mean = avg


def abs(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().abs())


def exp(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().exp())


def sin(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sin())


def asin(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().asin())


def sinh(col: ColumnOrName) -> Expr:
    return (exp(col) - exp(-col)) / 2


def cos(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().cos())


def acos(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().acos())


def cosh(col: ColumnOrName) -> Expr:
    return (exp(col) + exp(-col)) / 2


def tan(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().tan())


def atan(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().atan())


def atan2(col1: ColumnOrName | int | float, col2: ColumnOrName | int | float) -> Expr:
    if isinstance(col1, (int, float)):
        col1 = lit(col1)
    if isinstance(col2, (int, float)):
        col2 = lit(col2)
    return Expr(col_fn(col1).to_ibis().atan2(col_fn(col2).to_ibis()))


def tanh(col: ColumnOrName) -> Expr:
    return (exp(col) - exp(-col)) / (exp(col) + exp(-col))

def sec(col: ColumnOrName) -> Expr:
    return 1 / cos(col)

def csc(col: ColumnOrName) -> Expr:
    return 1 / sin(col)

def cot(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().cot())


def ln(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().ln())


def log(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().log())


def log10(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().log10())


def log2(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().log2())


def sqrt(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sqrt())


def degrees(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().degrees())


def radians(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().radians())


def e() -> Expr:
    return Expr(ibis.e)


def pi() -> Expr:
    return Expr(ibis.pi)


def ceil(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().ceil())


def floor(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().floor())


def negate(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().negate())


negative = negate

def sign(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sign())

signum = sign

def round(col: ColumnOrName, scale: int | None = None) -> Expr:
    return Expr(col_fn(col).to_ibis().round(scale))


def isnan(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().isnan())