from typing import Sequence

import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, lit
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn


@sql_func(col_name_args="col")
def avg(col: ColumnOrName) -> Expr:
    return col.mean()


mean = avg


@sql_func(col_name_args="col")
def bit_and(col: ColumnOrName) -> Expr:
    return col.bit_and()


@sql_func(col_name_args="col")
def bit_or(col: ColumnOrName) -> Expr:
    return col.bit_or()


@sql_func(col_name_args="col")
def bit_xor(col: ColumnOrName) -> Expr:
    return col.bit_xor()


@sql_func(col_name_args="col")
def bool_and(col: ColumnOrName) -> Expr:
    return col.all()


every = bool_and


@sql_func(col_name_args="col")
def bool_or(col: ColumnOrName) -> Expr:
    return col.any()


some = bool_or


@sql_func(col_name_args="col")
def collect_list(col: ColumnOrName) -> Expr:
    return col.collect(distinct=False)


@sql_func(col_name_args="col")
def collect_set(col: ColumnOrName) -> Expr:
    return col.collect(distinct=True)


@sql_func(col_name_args=("col1", "col2"))
def corr(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.corr(col2)


def count(col: ColumnOrName) -> Expr:
    if col == "*":
        return Expr(ibis.deferred.count()).alias("count(1)")
    return Expr(col_fn(col).to_ibis().count()).alias(f"count({col})")


@sql_func(col_name_args="col")
def count_if(col: ColumnOrName) -> Expr:
    return col.count(where=col)


@sql_func(col_name_args="col")
def first(col: ColumnOrName, ignorenulls: bool = False) -> Expr:
    return col.first(include_null=not ignorenulls)


@sql_func(col_name_args="col")
def last(col: ColumnOrName, ignorenulls: bool = False) -> Expr:
    return col.last(include_null=not ignorenulls)


@sql_func(col_name_args="col")
def max(col: ColumnOrName) -> Expr:
    return col.max()


# @sql_func(col_name_args=("col", "ord"))
# def max_by(col: ColumnOrName, ord: ColumnOrName) -> Expr:
#     return ord == ord.max()


@sql_func(col_name_args="col")
def min(col: ColumnOrName) -> Expr:
    return col.min()


# @sql_func(col_name_args=("col", "ord"))
# def min_by(col: ColumnOrName, ord: ColumnOrName) -> Expr:
#     return col.first(where=ord == ord.min())


@sql_func(col_name_args="col")
def median(col: ColumnOrName) -> Expr:
    return col.median()


@sql_func(col_name_args="col")
def mode(col: ColumnOrName) -> Expr:
    return col.mode()


@incompatibility("The frequency argument is not honored.")
@sql_func(col_name_args="col")
def percentile(
    col: ColumnOrName,
    percentage: Expr | float | Sequence[float],
    frequency: Expr | int = 1,
) -> Expr:
    percentage = lit(percentage).to_ibis()
    return col.quantile(percentage)


@incompatibility("The accuracy argument is not honored.")
@sql_func(col_name_args="col")
def approx_percentile(
    col: ColumnOrName,
    percentage: Expr | float | Sequence[float],
    accuracy: Expr | int = 10_000,
) -> Expr:
    percentage = lit(percentage).to_ibis()
    return col.approx_quantile(percentage)


percentile_approx = approx_percentile


# @sql_func(col_name_args="col")
# def product(col: ColumnOrName) -> Expr:
#     return col.prod()


@sql_func(col_name_args="col")
def stddev(col: ColumnOrName) -> Expr:
    return col.std()


std = stddev


@sql_func(col_name_args="col")
def sum(col: ColumnOrName) -> Expr:
    return col.sum()


@sql_func(col_name_args="col")
def kurtosis(col: ColumnOrName) -> Expr:
    return col.kurtosis(how="pop")


@sql_func(col_name_args="col")
def variance(col: ColumnOrName) -> Expr:
    return col.var()


try_avg = avg
try_sum = sum
