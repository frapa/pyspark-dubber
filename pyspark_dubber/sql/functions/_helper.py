import functools
from typing import Callable, Sequence

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.normal import col as col_fn, lit


def sql_func(
    func: Callable | None = None, *, col_name_args: Sequence[str] | str | None = None, do_not_print_args: Sequence[str] | str | None = None
) -> Callable:
    """Helper decorator that wraps the result in an Expr and ensures
    the expression is aliased to the function name.

    Additionally, the arguments marked at col_name_args are
    converted to ibis deferred expressions.
    """
    if col_name_args is None:
        col_name_args = ()
    elif isinstance(col_name_args, str):
        col_name_args = (col_name_args,)

    if do_not_print_args is None:
        do_not_print_args = ()
    elif isinstance(do_not_print_args, str):
        do_not_print_args = (do_not_print_args,)

    if func is None:

        def _decorator(func: Callable) -> Callable:
            return sql_func(func, col_name_args=col_name_args, do_not_print_args=do_not_print_args)

        return _decorator

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        args = list(args)
        pos_arg_names = list(func.__annotations__.keys())

        all_args = {**dict(zip(pos_arg_names, args)), **kwargs}
        arg_fmt = ", ".join(str(v) for k, v in all_args.items() if k not in do_not_print_args)

        for arg in col_name_args:
            if arg not in all_args:
                raise ValueError(
                    f"Column name {arg} (specified in col_name_args)"
                    f"is missing from function {func.__name__}"
                )

            if arg in pos_arg_names:
                idx = pos_arg_names.index(arg)
                val = args[idx]
                if isinstance(val, str):
                    args[idx] = col_fn(val).to_ibis()
                else:
                    args[idx] = lit(val).to_ibis()
            else:
                val = kwargs[arg]
                if isinstance(val, str):
                    kwargs[arg] = col_fn(val).to_ibis()
                else:
                    kwargs[arg] = lit(val).to_ibis()

        return Expr(func(*args, **kwargs)).alias(f"{func.__name__}({arg_fmt})")

    return _wrapper
