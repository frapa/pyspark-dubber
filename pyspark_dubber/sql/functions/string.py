import base64 as base64_lib
import sys
from collections.abc import Sequence
from typing import Callable, Any

import ibis
import ibis.expr.operations
import pyarrow

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions import col as col_fn
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName


def ascii(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().ascii_str())


def bit_length(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().length() * 8)


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
def find_in_set(str: str, strarray: Expr | str) -> Expr:
    return Expr(lit(strarray).to_ibis().find_in_set([str]) + 1)


def locate(substr: Expr | str, str: ColumnOrName, pos: ColumnOrName | int = 1) -> Expr:
    if isinstance(pos, int):
        pos = lit(pos)
    return Expr(col_fn(str).to_ibis().find(substr, start=col_fn(pos) - 1))


def instr(str: ColumnOrName, substr: Expr | str) -> Expr:
    return locate(substr, str)


def position(
    substr: Expr | str, str: ColumnOrName, start: ColumnOrName | int = 1
) -> Expr:
    return locate(substr, str, start)


def substr(
    str: ColumnOrName, pos: ColumnOrName | int, len: ColumnOrName | int | None = None
) -> Expr:
    if isinstance(pos, int):
        pos = lit(pos)
    if isinstance(len, int):
        len = lit(len)
    return col_fn(str).substr(col_fn(pos), col_fn(len))


substring = substr


def lower(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().lower())


lcase = lower


def upper(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().upper())


ucase = upper


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


def ltrim(col: ColumnOrName, trim: ColumnOrName | None = None) -> Expr:
    return _trim(
        lambda e: e.lstrip(),
        pyarrow.compute.utf8_ltrim,
        lambda s, t: s.lstrip(t),
        col,
        trim,
    )


def right(col: Expr | str, len: ColumnOrName | int) -> Expr:
    if isinstance(len, int):
        len = lit(len)
    return Expr(lit(col).to_ibis().right(col_fn(len).to_ibis()))


def rpad(col: ColumnOrName, len: Expr | int, pad: Expr | str) -> Expr:
    return Expr(col_fn(col).to_ibis().rpad(lit(len).to_ibis(), lit(pad).to_ibis()))


def rtrim(col: ColumnOrName, trim: ColumnOrName | None = None) -> Expr:
    return _trim(
        lambda e: e.rstrip(),
        pyarrow.compute.utf8_rtrim,
        lambda s, t: s.rstrip(t),
        col,
        trim,
    )


def _trim(
    ibis_func: Callable,
    pyarrow_op: Callable,
    python_func: Callable,
    col: ColumnOrName,
    trim: ColumnOrName | None = None,
) -> Expr:
    if trim is None:
        return Expr(ibis_func(col_fn(col).to_ibis()))

    trim = lit(trim).to_ibis()

    # If trim is a literal, we can use the more efficient pyarrow function
    if isinstance(trim.op(), ibis.expr.operations.Literal):
        trim = trim.op().value

        @ibis.udf.scalar.pyarrow
        def _trim(s: str) -> str:
            return pyarrow_op(s, trim)

        return Expr(_trim(col_fn(col).to_ibis()))

    @ibis.udf.scalar.python
    def _trim(s: str, trim: str) -> str:
        return python_func(s, trim)

    return Expr(_trim(col_fn(col).to_ibis(), lit(trim).to_ibis()))


def trim(col: ColumnOrName, trim: ColumnOrName | None = None) -> Expr:
    return _trim(
        lambda e: e.strip(),
        pyarrow.compute.utf8_trim,
        lambda s, t: s.strip(t),
        col,
        trim,
    )


# Could not spot any difference
btrim = trim


@incompatibility("The `seed` argument is not honored. Output is lowercase-only.")
def randstr(length: Expr | int, seed: Expr | int | None = None) -> Expr:
    # TODO: validate length, that can only be either 16 or 32
    return Expr(
        (ibis.random() * sys.maxsize).cast("str").hexdigest().substr(1, length)
    ).alias(f"randstr({length}, {seed})")


def regexp_count(str: ColumnOrName, pattern: ColumnOrName) -> Expr:
    return Expr(col_fn(str).to_ibis().re_split(col_fn(pattern).to_ibis()).length() - 1)


def regexp_extract(str: ColumnOrName, pattern: str, idx: int) -> Expr:
    return Expr(col_fn(str).to_ibis().re_extract(lit(pattern).to_ibis(), idx))


@sql_func(col_name_args=("str", "regexp"))
def regexp_extract_all(str: ColumnOrName, regexp: ColumnOrName, idx: Expr | int = 1) -> Expr:
    return ibis.array([str.re_extract(regexp, lit(idx).to_ibis())])


def repeat(col: ColumnOrName, n: ColumnOrName | int) -> Expr:
    if isinstance(n, int):
        n = lit(n)
    n = col_fn(n)
    return Expr(col_fn(col).to_ibis().repeat(n))


def replace(src: Expr | str, search: Expr | str, replace: Expr | str = "") -> Expr:
    src = lit(src).to_ibis()
    search = lit(search).to_ibis()
    replace = lit(replace).to_ibis()
    return Expr(src.replace(search, replace))


@incompatibility("The `limit` argument is not honored.")
def split(
    str: ColumnOrName, pattern: Expr | str, limit: ColumnOrName | int = -1
) -> Expr:
    return Expr(col_fn(str).to_ibis().split(lit(pattern).to_ibis()))


def split_part(
    str: ColumnOrName, delimiter: ColumnOrName, partNum: ColumnOrName
) -> Expr:
    str_ = col_fn(str)
    del str  # restore builtin
    delimiter = col_fn(delimiter)
    partNum = col_fn(partNum)
    # As usual, spark is 1-based
    part_zero_based = ibis.ifelse(
        partNum.to_ibis() >= 0,
        partNum.to_ibis() - 1,
        partNum.to_ibis(),
    )
    return Expr(str_.to_ibis().split(delimiter.to_ibis())[part_zero_based]).alias(
        f"split_part({str_}, {delimiter}, {partNum})"
    )


@incompatibility("Negative counts are not supported.")
def substring_index(str: ColumnOrName, delim: str, count: int) -> Expr:
    str_ = col_fn(str)
    del str  # restore builtin
    parts = str_.to_ibis().split(delim)
    if count < 0:
        raise NotImplementedError("Negative counts are not supported.")
    else:
        filter_func = lambda _, i: i < count

    return Expr(parts.filter(filter_func).join(delim)).alias(
        f"substring_index({str_}, {delim}, {count})"
    )


def to_binary(col: Expr | str, format: ColumnOrName = lit("hex")) -> Expr:
    format = col_fn(format)

    # If trim is a literal, we can use the more efficient pyarrow function
    if isinstance(format.to_ibis().op(), ibis.expr.operations.Literal):
        format_str = format.to_ibis().op().value.lower()
        if format_str == "utf8" or format_str == "utf-8":
            return Expr(lit(col).to_ibis().cast("binary")).alias(
                f"to_binary({col}, {format})"
            )

    @ibis.udf.scalar.python
    def to_binary(data: str, format: str) -> bytes:
        format = format.lower()

        if format == "hex":
            if len(data) % 2 != 0:
                # pad with zero if it's not complete bytes
                data = f"0{data}"
            return bytes.fromhex(data)
        if format == "utf8" or format == "utf-8":
            return data.encode("utf-8")

        padding = '=' * (len(data) % 4)
        data = f"{data}{padding}"
        return base64_lib.b64decode(data.encode())

    return Expr(to_binary(lit(col).to_ibis(), format.to_ibis())).alias(
        f"to_binary({col}, {format})"
    )


def translate(srcCol: ColumnOrName, matching: str, replace: str) -> Expr:
    missing = set(matching) - set(replace)
    src = col_fn(srcCol).to_ibis()
    for c in missing:
        src.replace(c, "")
    return Expr(src.translate(lit(matching).to_ibis(), lit(replace).to_ibis()))


def base64(col: ColumnOrName) -> Expr:
    @ibis.udf.scalar.python
    def _base64_encode(data: bytes) -> str:
        return base64_lib.b64encode(data).decode()

    col_bin = col_fn(col).to_ibis().cast("binary")
    return Expr(_base64_encode(col_bin)).alias(f"base64({col})")


def unbase64(col: ColumnOrName) -> Expr:
    @ibis.udf.scalar.python
    def _base64_decode(data: bytes) -> bytes:
        return base64_lib.b64decode(data)

    col_bin = col_fn(col).to_ibis().cast("binary")
    return Expr(_base64_decode(col_bin)).alias(f"unbase64({col})")


@sql_func(col_name_args="format")
def printf(format: ColumnOrName, *cols: ColumnOrName) -> Expr:
    cols = [col_fn(c).to_ibis() for c in cols]

    @ibis.udf.scalar.python
    def _printf(format: str, *cols: str) -> str:
        return format % cols

    return _printf(format, *cols)


def format_string(format: str, *cols: ColumnOrName) -> Expr:
    return printf(lit(format), *cols)