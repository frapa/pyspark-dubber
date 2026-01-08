from functools import cache
from typing import Callable

import ibis
import sqlglot

from pyspark_dubber.sql import functions
from pyspark_dubber.sql.expr import Expr, lit

ColumnOrName = Expr | str


def _col_fn(col: ColumnOrName) -> Expr:
    if isinstance(col, Expr):
        return col
    parts = col.split(".")
    e = Expr(ibis.deferred[parts[0]])
    for attr in parts[1:]:
        e = e[attr]
    return e


col = _col_fn
column = _col_fn
lit = lit


@cache
def functions_in_lowercase() -> dict[str, Callable]:
    return {
        name.lower(): getattr(functions, name)
        for name in functions.__all__
        if not name.startswith("_") and callable(getattr(functions, name))
    }


def call_function(funcName: str, *cols: ColumnOrName) -> Expr:
    funcName = funcName.lower()
    func = functions_in_lowercase().get(funcName)
    if func is None:
        raise ValueError(f"Function '{funcName}' not found.")

    args_str = ", ".join(map(str, cols))
    return func(*[_col_fn(c) for c in cols])


def expr(str: str) -> Expr:
    ast = sqlglot.parse_one(str, dialect="spark")
    return Expr(_build_ibis_expr(ast))


def _build_ibis_expr(ast: sqlglot.Expression) -> ibis.Value | ibis.Deferred:
    # Order is important because, for example, expressions and case statements are
    # subclasses of Func in sqlglot.
    if isinstance(ast, sqlglot.expressions.Case):
        conditions = ast.args["ifs"]
        default = ast.args.get("default")
        return ibis.cases(
            *[
                (_build_ibis_expr(cond.this), _build_ibis_expr(cond.args["true"]))
                for cond in conditions
            ],
            else_=default and _build_ibis_expr(default),
        )

    if isinstance(ast, sqlglot.expressions.Binary):
        left = _build_ibis_expr(ast.left)
        right = _build_ibis_expr(ast.right)
        match ast:
            case sqlglot.expressions.Add():
                return left + right
            case sqlglot.expressions.Sub():
                return left - right
            case sqlglot.expressions.Mul():
                return left * right
            case sqlglot.expressions.Div():
                return left / right
            case sqlglot.expressions.Mod():
                return left % right
            case sqlglot.expressions.EQ():
                return left == right
            case sqlglot.expressions.GT():
                return left > right
            case sqlglot.expressions.GTE():
                return left >= right
            case sqlglot.expressions.LT():
                return left < right
            case sqlglot.expressions.LTE():
                return left <= right
            case sqlglot.expressions.And():
                return left.cast(bool) & right.cast(bool)
            case sqlglot.expressions.BitwiseAnd():
                return left & right
            case sqlglot.expressions.Or():
                return left.cast(bool) | right.cast(bool)
            case sqlglot.expressions.BitwiseOr():
                return left | right
            case sqlglot.expressions.Xor():
                return left ^ right
            case sqlglot.expressions.Is():
                return left.identical_to(right)
            case sqlglot.expressions.In():
                return left.isin(right)
            case _:
                raise NotImplementedError(
                    f"Binary operator '{ast.sql(dialect='spark')}' not implemented."
                )

    if isinstance(ast, sqlglot.expressions.Unary):
        value = _build_ibis_expr(ast.this)
        match ast:
            case sqlglot.expressions.Neg():
                return value.negate()
            case sqlglot.expressions.Not() | sqlglot.expressions.BitwiseNot():
                return ~value
            case _:
                raise NotImplementedError(
                    f"Unary operator '{ast.sql(dialect='spark')}' not implemented."
                )

    if isinstance(ast, sqlglot.expressions.Func):
        return call_function(
            ast.sql_name(), *[Expr(_build_ibis_expr(a)) for a in ast.args.values()]
        ).to_ibis()

    if isinstance(ast, sqlglot.expressions.Column):
        return _build_ibis_expr(ast.this)

    if isinstance(ast, sqlglot.expressions.Identifier):
        return ibis.deferred[ast.name]

    if isinstance(ast, sqlglot.expressions.Literal):
        value = ast.to_py()
        if isinstance(value, int):
            return ibis.literal(value).cast("int32")
        return ibis.literal(value)

    if isinstance(ast, sqlglot.expressions.Null):
        return ibis.null()

    raise NotImplementedError(
        f"Parsing of expression '{ast.sql(dialect='spark')}' not implemented:\n{repr(ast)}"
    )
