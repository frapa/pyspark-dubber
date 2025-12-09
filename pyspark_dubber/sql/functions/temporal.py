from datetime import date, datetime
from typing import Literal

import ibis
import tzlocal

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, lit, col as col_fn


@sql_func(col_name_args=("start", "months"))
def add_months(start: ColumnOrName, months: ColumnOrName | int) -> Expr:
    return start + months.as_interval("M")


def current_date() -> Expr:
    return lit(date.today())


curdate = current_date


def current_timestamp() -> Expr:
    return lit(datetime.now())


localtimestamp = current_timestamp
now = current_timestamp


def current_timezone() -> Expr:
    return lit(tzlocal.get_localzone_name())


@sql_func(col_name_args=("start", "days"))
def date_add(start: ColumnOrName, days: ColumnOrName | int) -> Expr:
    return start + days.as_interval("D")


dateadd = date_add


@sql_func(col_name_args=("end", "start"))
def date_diff(end: ColumnOrName, start: ColumnOrName) -> Expr:
    return (end - start).days.cast("int")


datediff = date_diff


@sql_func(col_name_args=("start", "days"))
def date_sub(start: ColumnOrName, days: ColumnOrName | int) -> Expr:
    return start - days.as_interval("D")


# @sql_func(col_name_args=("date"))
# def date_format(date: ColumnOrName, format: str) -> Expr:
#     return date.to_ibis().strftime(fmt)


@sql_func(col_name_args="col")
def year(col: ColumnOrName) -> Expr:
    return col.year()


@sql_func(col_name_args="col")
def quarter(col: ColumnOrName) -> Expr:
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


@sql_func(col_name_args="col")
def monthname(col: ColumnOrName) -> Expr:
    return col.strftime("%b")


@sql_func(col_name_args="col")
def dayofyear(col: ColumnOrName) -> Expr:
    return col.day_of_year()


day_of_month = day


@sql_func(col_name_args="col")
def dayofweek(col: ColumnOrName) -> Expr:
    return (col.day_of_week.index() + 1) % 7 + 1


@sql_func(col_name_args="col")
def weekday(col: ColumnOrName) -> Expr:
    return col.day_of_week.index()


@sql_func(col_name_args="col")
def weekofyear(col: ColumnOrName) -> Expr:
    return col.week_of_year()


@incompatibility(
    "Currently the `tz` timezone argument is ignored, "
    "therefore this function is mostly useless."
)
@sql_func(col_name_args="col")
def from_utc_timestamp(timestamp: ColumnOrName, tz: Expr | str) -> Expr:
    return timestamp.cast("timestamp")


TruncFmt = Literal[
    "year",
    "yyyy",
    "yy",
    "month",
    "mon",
    "mm",
    "day",
    "dd",
    "microsecond",
    "millisecond",
    "second",
    "minute",
    "hour",
    "week",
    "quarter",
]


@sql_func(col_name_args="date")
def trunc(date: ColumnOrName, format: TruncFmt) -> Expr:
    ibis_unit = {
        "year": "Y",
        "yyyy": "Y",
        "yy": "Y",
        "month": "M",
        "mon": "M",
        "mm": "M",
        "day": "D",
        "dd": "D",
        "microsecond": "us",
        "millisecond": "ms",
        "second": "s",
        "minute": "m",
        "hour": "h",
        "week": "W",
        "quarter": "Q",
    }[format.lower()]
    return date.truncate(ibis_unit)


def date_trunc(format: TruncFmt, timestamp: ColumnOrName) -> Expr:
    return trunc(timestamp, format)


def last_day(date: ColumnOrName) -> Expr:
    return add_months(trunc(date, "M"), 1) - ibis.interval(days=1)


@sql_func(col_name_args=("year", "month", "day"))
def make_date(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Expr:
    return ibis.date(year, month, day)


@sql_func(col_name_args=("years", "months", "days", "hours", "mins", "secs"))
def make_timestamp(
    years: ColumnOrName,
    months: ColumnOrName,
    days: ColumnOrName,
    hours: ColumnOrName,
    mins: ColumnOrName,
    secs: ColumnOrName,
    timezone: ColumnOrName | None = None,
) -> Expr:
    return ibis.timestamp(years, months, days, hours, mins, secs, timezone)


make_timestamp_ntz = make_timestamp
make_timestamp_ltz = make_timestamp
try_make_timestamp = make_timestamp
try_make_timestamp_ntz = make_timestamp
try_make_timestamp_ltz = make_timestamp


@incompatibility("The parameter roundOff is not honored.")
@sql_func(col_name_args=("date1", "date2"))
def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: bool = True
) -> Expr:
    return (day(date1) == day(date2)).ifelse(
        (date2 - date1).months, (date2 - date1).days / 31
    )


def next_day(
    date: ColumnOrName,
    dayOfWeek: Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
) -> Expr:
    target = {"Sun": 1, "Mon": 2, "Tue": 3, "Wed": 4, "Thu": 5, "Fri": 6, "Sat": 7}[
        dayOfWeek.upper()
    ]
    return date_add(col_fn(date), (target - dayofweek(date)) % 7)


DateTimeUnits = Literal[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
]
DATE_TIME_UNIT_TO_IBIS = {
    "year": "Y",
    "quarter": "Q",
    "month": "M",
    "week": "W",
    "day": "D",
    "hour": "h",
    "minute": "m",
    "second": "s",
    "millisecond": "ms",
    "microsecond": "us",
}


@sql_func(col_name_args=("quantity", "ts"))
def timestamp_add(
    unit: DateTimeUnits,
    quantity: ColumnOrName,
    ts: ColumnOrName,
) -> Expr:
    unit = DATE_TIME_UNIT_TO_IBIS[unit.lower()]
    return ts + ibis.interval(quantity, unit)


@sql_func(col_name_args=("start", "end"))
def timestamp_diff(unit: DateTimeUnits, start: ColumnOrName, end: ColumnOrName) -> Expr:
    unit = DATE_TIME_UNIT_TO_IBIS[unit.lower()]
    return (end - start).as_unit(unit)


@sql_func(col_name_args="days")
def date_from_unix_date(days: ColumnOrName) -> Expr:
    return ibis.date(1970, 1, 1) + days.as_interval("D")


@sql_func(col_name_args="col")
def timestamp_seconds(col: ColumnOrName) -> Expr:
    return ibis.timestamp(1970, 1, 1, 0, 0, 0) + col.as_interval("s")


@sql_func(col_name_args="col")
def timestamp_millis(col: ColumnOrName) -> Expr:
    return ibis.timestamp(1970, 1, 1, 0, 0, 0) + col.as_interval("ms")


@sql_func(col_name_args="col")
def timestamp_micros(col: ColumnOrName) -> Expr:
    return ibis.timestamp(1970, 1, 1, 0, 0, 0) + col.as_interval("us")


@sql_func(col_name_args="col")
def unix_date(col: ColumnOrName) -> Expr:
    return (col - ibis.date(1970, 1, 1)).as_unit("D")


@sql_func(col_name_args="col")
def unix_seconds(col: ColumnOrName) -> Expr:
    return (col - ibis.timestamp(1970, 1, 1, 0, 0, 0)).as_unit("s")


@sql_func(col_name_args="col")
def unix_millis(col: ColumnOrName) -> Expr:
    return (col - ibis.timestamp(1970, 1, 1, 0, 0, 0)).as_unit("ms")


@sql_func(col_name_args="col")
def unix_micros(col: ColumnOrName) -> Expr:
    return (col - ibis.timestamp(1970, 1, 1, 0, 0, 0)).as_unit("us")
