import dataclasses

import ibis.expr.types


# Implemented here to avoid circular imports
# TODO: support numpy arrays
def lit(
    col: "str | int | float | bool | list[str | int | float | bool] | Expr",
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

    def asc(self) -> "Expr":
        return Expr(self._ibis_expr.asc())

    def desc(self) -> "Expr":
        return Expr(self._ibis_expr.desc())

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

