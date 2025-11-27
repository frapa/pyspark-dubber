from pyspark_dubber.sql.column import Column
from pyspark_dubber.sql.functions.base import ColumnOrName


def to_timestamp(col: ColumnOrName, format: str | None = None) -> Column:
    return Column()


def current_timestamp() -> Column:
    return Column()
