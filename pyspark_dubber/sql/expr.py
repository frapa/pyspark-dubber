import dataclasses
from datetime import date, datetime

import ibis.expr.types
from ibis.expr.operations import scalar

from pyspark_dubber.sql.types import DataType

ScalarValue = str | int | float | bool | date | datetime


# Implemented here to avoid circular imports
# TODO: support numpy arrays
def lit(
    col: "ScalarValue | list[ScalarValue] | Expr",
) -> "Expr":
    if isinstance(col, Expr):
        return col
    return Expr(ibis.literal(col))


@dataclasses.dataclass
class Expr:
    _ibis_expr: ibis.expr.types.Value | ibis.Deferred

    def to_ibis(self) -> ibis.expr.types.Value | ibis.Deferred:
        return self._ibis_expr

    def alias(self, alias: str) -> "Expr":
        return Expr(self._ibis_expr.name(alias))

    name = alias

    def asc_nulls_first(self) -> "Expr":
        return Expr(self._ibis_expr.asc_nulls_first())

    def asc_nulls_last(self) -> "Expr":
        return Expr(self._ibis_expr.asc_nulls_last())

    asc = asc_nulls_first

    def desc_nulls_first(self) -> "Expr":
        return Expr(self._ibis_expr.desc_nulls_first())

    def desc_nulls_last(self) -> "Expr":
        return Expr(self._ibis_expr.desc_nulls_last())

    desc = desc_nulls_first

    def cast(self, data_type: DataType) -> "Expr":
        return Expr(self._ibis_expr.cast(data_type.to_ibis()))

    astype = cast

    def between(
        self, lower: "Expr | ScalarValue", upper: "Expr | ScalarValue"
    ) -> "Expr":
        if isinstance(lower, ScalarValue):
            lower = lit(lower)
        if isinstance(upper, ScalarValue):
            upper = lit(upper)
        return Expr(
            self._ibis_expr.between(lower.to_ibis(), upper.to_ibis()).name(
                f"(({self} >= {lower}) AND ({self} <= {upper}))"
            )
        )

    def bitwiseAND(self, other: "Expr") -> "Expr":
        return Expr(
            self._ibis_expr.bit_and(other.to_ibis()).name(f"({self} & {other})")
        )

    def bitwiseOR(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr.bit_or(other.to_ibis()).name(f"({self} | {other})"))

    def bitwiseXOR(self, other: "Expr") -> "Expr":
        return Expr(
            self._ibis_expr.bit_xor(other.to_ibis()).name(f"({self} ^ {other})")
        )

    def contains(self, other: "Expr | str") -> "Expr":
        if isinstance(other, str):
            other = lit(other)
        return Expr(self._ibis_expr.contains(other.to_ibis()))

    def startswith(self, other: "Expr | str") -> "Expr":
        if isinstance(other, str):
            other = lit(other)
        return Expr(self._ibis_expr.startswith(other.to_ibis()))

    def endswith(self, other: "Expr | str") -> "Expr":
        if isinstance(other, str):
            other = lit(other)
        return Expr(self._ibis_expr.endswith(other.to_ibis()))

    def isNull(self) -> "Expr":
        return Expr(self._ibis_expr.isnull())

    def isNotNull(self) -> "Expr":
        return Expr(self._ibis_expr.notnull())

    def substr(self, startPos: "Expr | int", length: "Expr | int") -> "Expr":
        return Expr(self._ibis_expr.substr(startPos, length))

    def like(self, pattern: str) -> "Expr":
        return Expr(self._ibis_expr.like(pattern))

    def rlike(self, pattern: str) -> "Expr":
        return Expr(self._ibis_expr.rlike(pattern))

    def ilike(self, pattern: str) -> "Expr":
        return Expr(self._ibis_expr.ilike(pattern))

    def __eq__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr == lit(other).to_ibis())

    def __lt__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr < lit(other).to_ibis())

    def __le__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr <= lit(other).to_ibis())

    def __gt__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr > lit(other).to_ibis())

    def __ge__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr >= lit(other).to_ibis())

    def __neg__(self) -> "Expr":
        return Expr(-self._ibis_expr)

    def __add__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return Expr(self._ibis_expr + other.to_ibis())

    def __sub__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return Expr(self._ibis_expr - other.to_ibis())

    def __mul__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return Expr(self._ibis_expr * other.to_ibis())

    def __truediv__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return Expr(self._ibis_expr / other.to_ibis())

    def __radd__(self, other: "Expr | int | float") -> "Expr":
        return other + self

    def __rsub__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return other - self

    def __rmul__(self, other: "Expr | int | float") -> "Expr":
        return other * self

    def __rtruediv__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return other / self
