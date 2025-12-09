from datetime import date, datetime, timezone

import ibis
import tzlocal

from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions import lit
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName


@sql_func(col_name_args=("start", "months"))
def add_months(start: ColumnOrName, months: ColumnOrName | int) -> Expr:
    return start + months * ibis.interval(months=1)


def current_date() -> Expr:
    return lit(date.today())


curdate = current_date


def current_timestamp() -> Expr:
    return lit(datetime.now())


def current_timezone() -> Expr:
    return lit(tzlocal.get_localzone_name())


@sql_func(col_name_args=("start", "days"))
def date_add(start: ColumnOrName, days: ColumnOrName | int) -> Expr:
    return start + days * ibis.interval(days=1)


dateadd = date_add


@sql_func(col_name_args=("end", "start"))
def date_diff(end: ColumnOrName, start: ColumnOrName) -> Expr:
    return (end - start).days.cast("int")


datediff = date_diff


@sql_func(col_name_args=("start", "days"))
def date_sub(start: ColumnOrName, days: ColumnOrName | int) -> Expr:
    return start - days * ibis.interval(days=1)


# @sql_func(col_name_args=("date"))
# def date_format(date: ColumnOrName, format: str) -> Expr:
#     return date.to_ibis().strftime(fmt)


# @sql_func(col_name_args="days")
# def date_from_unix_date(days: ColumnOrName) -> Expr:
#     return ibis.date(1970, 1, 1) + days * ibis.interval(days=1)


@sql_func(col_name_args="col")
def year(col: ColumnOrName) -> Expr:
    return col.year()


@sql_func(col_name_args="col")
def querter(col: ColumnOrName) -> Expr:
    return col.quarter()


@sql_func(col_name_args="col")
def month(col: ColumnOrName) -> Expr:
    return col.month()


@sql_func(col_name_args="col")
def day(col: ColumnOrName) -> Expr:
    return col.day()


@sql_func(col_name_args="col")
def hour(col: ColumnOrName) -> Expr:
    return col.hour()


@sql_func(col_name_args="col")
def minute(col: ColumnOrName) -> Expr:
    return col.minute()


@sql_func(col_name_args="col")
def second(col: ColumnOrName) -> Expr:
    return col.second()


@sql_func(col_name_args="col")
def dayname(col: ColumnOrName) -> Expr:
    return col.day_of_week.full_name()[:3]


# @sql_func(col_name_args="col")
# def monthname(col: ColumnOrName) -> Expr:
#     return col.day_of_week


@sql_func(col_name_args="col")
def dayofyear(col: ColumnOrName) -> Expr:
    return col.day_of_year()


day_of_month = day


@sql_func(col_name_args="col")
def dayofweek(col: ColumnOrName) -> Expr:
    return (col.day_of_week.index() + 1) % 7 + 1