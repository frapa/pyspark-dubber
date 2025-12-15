import functools
from typing import Callable, Sequence

import ibis

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import col as col_fn, lit


def sql_func(
    func: Callable | None = None, *, col_name_args: Sequence[str] | str | None = None
) -> Callable:
    """Helper decorator that wraps the result in and Expr and ensures
    the expression is aliased to the function name.

    Additionally, the arguments marked at col_name_args are
    converted to ibis deferred expressions.
    """
    if col_name_args is None:
        col_name_args = ()
    elif isinstance(col_name_args, str):
        col_name_args = (col_name_args,)

    if func is None:

        def _decorator(func: Callable) -> Callable:
            return sql_func(func, col_name_args=col_name_args)

        return _decorator

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        kwargs.update(dict(zip(func.__annotations__.keys(), args)))
        arg_fmt = ", ".join(map(str, kwargs.values()))

        for arg in col_name_args:
            if arg not in kwargs:
                raise ValueError(
                    f"Column name {arg} (specified in col_name_args)"
                    f"is missing from function {func.__name__}"
                )

            if kwargs[arg] is None:
                kwargs[arg] = None
            if not isinstance(kwargs[arg], str):
                kwargs[arg] = lit(kwargs[arg])
            kwargs[arg] = col_fn(kwargs[arg]).to_ibis()

        return Expr(func(**kwargs)).alias(f"{func.__name__}({arg_fmt})")

    return _wrapper
