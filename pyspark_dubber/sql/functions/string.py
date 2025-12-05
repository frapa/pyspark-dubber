import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions import col as col_fn
from pyspark_dubber.sql.functions.normal import ColumnOrName


def ascii(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().ascii_str())


def bit_length(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().length() * 8)


# def btrim(col: ColumnOrName) -> Expr:
#     return Expr(col_fn(col).to_ibis().strip())


# TODO: Untested
def char(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().cast("string"))


def char_length(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().length())


character_length = char_length
length = char_length


def concat(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("concat requires at least one column")
    return Expr(col_fn(cols[0]).to_ibis().concat(col_fn(c).to_ibis() for c in cols[1:]))


def concat_ws(sep: str, *cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("concat_ws requires at least one column")
    return Expr(lit(sep).to_ibis().join(col_fn(c).to_ibis() for c in cols))


def contains(left: Expr | str, right: Expr | str) -> Expr:
    return lit(left).contains(right)


def startswith(col: Expr | str, prefix: Expr | str) -> Expr:
    return lit(col).startswith(prefix)


def endswith(col: Expr | str, suffix: Expr | str) -> Expr:
    return lit(col).endswith(suffix)


# TODO: untested
@incompatibility(
    "find_in_set only supports strings as the first argument, "
    "not dynamically another column like in pyspark."
)
def find_in_set(str_: str, str_array: Expr | str) -> Expr:
    return Expr(lit(str_array).to_ibis().find_in_set([str_]) + 1)


def locate(substr: Expr | str, str_: ColumnOrName, pos: ColumnOrName | int = 1) -> Expr:
    if isinstance(pos, int):
        pos = lit(pos)
    return Expr(col_fn(str_).to_ibis().find(substr, start=col_fn(pos) - 1))


def instr(str_: ColumnOrName, substr: Expr | str) -> Expr:
    return locate(substr, str_)


def position(
    substr: Expr | str, str_: ColumnOrName, start: ColumnOrName | int = 1
) -> Expr:
    return locate(substr, str_, start)


def substr(
    str_: ColumnOrName, pos: ColumnOrName | int, len: ColumnOrName | int | None = None
) -> Expr:
    if isinstance(pos, int):
        pos = lit(pos)
    if isinstance(len, int):
        len = lit(len)
    return col_fn(str_).substr(col_fn(pos), col_fn(len))


substring = substr


def lcase(col: Expr | str) -> Expr:
    return Expr(lit(col).to_ibis().lower())


def lower(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().lower())


def levenshtein(
    left: ColumnOrName, right: ColumnOrName, threshold: int | None = None
) -> Expr:
    dist = col_fn(left).to_ibis().levenshtein(col_fn(right).to_ibis())
    if threshold is not None:
        dist = (dist <= threshold).ifelse(dist, ibis.literal(-1))
    return Expr(dist)


def left(col: Expr | str, len: ColumnOrName | int) -> Expr:
    if isinstance(len, int):
        len = lit(len)
    return Expr(lit(col).to_ibis().left(col_fn(len).to_ibis()))


def lpad(col: ColumnOrName, len: Expr | int, pad: Expr | str) -> Expr:
    return Expr(col_fn(col).to_ibis().lpad(lit(len).to_ibis(), lit(pad).to_ibis()))


def right(col: Expr | str, len: ColumnOrName | int) -> Expr:
    if isinstance(len, int):
        len = lit(len)
    return Expr(lit(col).to_ibis().right(col_fn(len).to_ibis()))


def rpad(col: ColumnOrName, len: Expr | int, pad: Expr | str) -> Expr:
    return Expr(col_fn(col).to_ibis().rpad(lit(len).to_ibis(), lit(pad).to_ibis()))
