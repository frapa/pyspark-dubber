import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn
from pyspark_dubber.sql.functions.normal import lit, _col_fn


def avg(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().mean())


mean = avg


def abs(col: ColumnOrName) -> Expr:
    col = _col_fn(col)
    return Expr(col.to_ibis().abs()).alias(f"abs({col})")


def exp(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().exp())


def sin(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sin())


def asin(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().asin())


def sinh(col: ColumnOrName) -> Expr:
    return (exp(col) - exp(-col)) / 2


def asinh(col: ColumnOrName) -> Expr:
    return ln(col + sqrt(col**2 + 1))


def cos(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().cos())


def acos(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().acos())


def cosh(col: ColumnOrName) -> Expr:
    return (exp(col) + exp(-col)) / 2


def acosh(col: ColumnOrName) -> Expr:
    return ln(col + sqrt(col**2 - 1))


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


def atanh(col: ColumnOrName) -> Expr:
    return 0.5 * ln((1 + col) / (1 - col))


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


ceiling = ceil


def floor(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().floor())


def positive(col: ColumnOrName) -> Expr:
    return _col_fn(col)


def negate(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().negate())


negative = negate


def sign(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sign())


signum = sign


def round(col: ColumnOrName, scale: int | None = None) -> Expr:
    return Expr(col_fn(col).to_ibis().round(scale))


rint = round


def pow(col1: ColumnOrName | int | float, col2: ColumnOrName | int | float) -> Expr:
    return col_fn(col1) ** col_fn(col2)


power = pow


def sqrt(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().sqrt())


def cbrt(col: ColumnOrName) -> Expr:
    return pow(col, 1 / 3)


@incompatibility(
    "The seed value is accepted for API compatibility, "
    "but is unused. Even if set, the function will not be reproducible."
)
def rand(seed: int | None = None) -> Expr:
    return Expr(ibis.random())


randn = rand


def uniform(
    min: Expr | int | float, max: Expr | int | float, seed: int | None = None
) -> Expr:
    if isinstance(min, (int, float)):
        min = lit(min)
    if isinstance(max, (int, float)):
        max = lit(max)
    return rand(seed) * (max - min) + min


def pmod(
    dividend: ColumnOrName | int | float, divisor: ColumnOrName | int | float
) -> Expr:
    if isinstance(dividend, (int, float)):
        dividend = lit(dividend)
    if isinstance(divisor, (int, float)):
        divisor = lit(divisor)
    return col_fn(dividend) % col_fn(divisor)


def greatest(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("At least one column must be provided to greatest()")
    return Expr(ibis.greatest(_col_fn(c).to_ibis() for c in cols))


def least(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("At least one column must be provided to least()")
    return Expr(ibis.least(_col_fn(c).to_ibis() for c in cols))
