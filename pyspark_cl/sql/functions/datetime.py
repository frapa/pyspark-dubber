from pyspark_cl.sql import Column
from pyspark_cl.sql.functions.base import ColumnOrName


def to_timestamp(col: ColumnOrName, format: str | None = None) -> Column:
    return Column()


def current_timestamp() -> Column:
    return Column()
