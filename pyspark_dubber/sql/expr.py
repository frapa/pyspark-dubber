import dataclasses
from datetime import date, datetime

import ibis.expr.types
from ibis.expr.operations import scalar

from pyspark_dubber.sql.types import DataType

ScalarValue = str | int | float | bool | date | datetime
LiteralValue = ScalarValue | list[ScalarValue]


# Implemented here to avoid circular imports
# TODO: support numpy arrays
def lit(
    col: "LiteralValue | Expr",
) -> "Expr":
    if isinstance(col, Expr):
        return col
    return Expr(ibis.literal(col))


@dataclasses.dataclass
class Expr:
    _ibis_expr: ibis.expr.types.Value | ibis.Deferred | None

    def to_ibis(self) -> ibis.expr.types.Value | ibis.Deferred:
        return self._ibis_expr

    def alias(self, alias: str) -> "Expr":
        return Expr(self._ibis_expr.name(alias))

    name = alias

    def asc_nulls_first(self) -> "Expr":
        return Expr(self._ibis_expr.asc_nulls_first())

    def asc_nulls_last(self) -> "Expr":
        return Expr(self._ibis_expr.asc_nulls_last())

    def asc(self) -> "Expr":
        return Expr(self._ibis_expr.asc())

    def desc_nulls_first(self) -> "Expr":
        return Expr(self._ibis_expr.desc_nulls_first())

    def desc_nulls_last(self) -> "Expr":
        return Expr(self._ibis_expr.desc_nulls_last())

    def desc(self) -> "Expr":
        return Expr(self._ibis_expr.desc())

    def cast(self, data_type: DataType | str) -> "Expr":
        if isinstance(data_type, str):
            data_type = DataType.from_DDL(data_type)
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

    def eqNullSafe(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr.identical_to(other.to_ibis()))

    def __eq__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr == lit(other).to_ibis())

    def __ne__(self, other: "Expr") -> "Expr":
        return Expr(self._ibis_expr != lit(other).to_ibis())

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

    def __div__(self, other: "Expr | int | float") -> "Expr":
        return self.__truediv__(other)

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

    def __rdiv__(self, other: "Expr | int | float") -> "Expr":
        return self.__rtruediv__(other)

    def __rtruediv__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return other / self

    def __mod__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return Expr(self._ibis_expr % other.to_ibis())

    def __rmod__(self, other: "Expr | int | float") -> "Expr":
        if isinstance(other, (int, float)):
            other = lit(other)
        return other % self

    def __pow__(self, exponent: "Expr | int | float") -> "Expr":
        if isinstance(exponent, (int, float)):
            exponent = lit(exponent)
        return Expr(self._ibis_expr ** exponent.to_ibis())

    def __rpow__(self, base: "Expr | int | float") -> "Expr":
        if isinstance(base, (int, float)):
            base = lit(base)
        return base**self

    def __and__(self, other: "Expr | int") -> "Expr":
        return Expr(self._ibis_expr & other.to_ibis())

    def __or__(self, other: "Expr | int") -> "Expr":
        return Expr(self._ibis_expr | other.to_ibis())

    def __xor__(self, other: "Expr | int") -> "Expr":
        return Expr(self._ibis_expr ^ other.to_ibis())

    def __invert__(self) -> "Expr":
        return Expr(~self._ibis_expr)


@dataclasses.dataclass
class WhenExpr(Expr):
    branches: list[tuple["Expr", "Expr"]]

    def when(self, condition: "Expr", value: "Expr | LiteralValue") -> "WhenExpr":
        return WhenExpr(self._ibis_expr, [*self.branches, (condition, value)])

    def otherwise(self, value: "Expr | LiteralValue") -> "Expr":
        return Expr(
            ibis.cases(
                *[(c.to_ibis(), v.to_ibis()) for c, v in self.branches],
                else_=value.to_ibis(),
            )
        )

    def to_ibis(self) -> ibis.expr.types.Value | ibis.Deferred:
        return ibis.cases(*[(c.to_ibis(), v.to_ibis()) for c, v in self.branches])
