from pyspark_dubber.sql.column import Column, RefColumn

ColumnOrName = Column | str


def col(col: str) -> Column:
    return RefColumn(col)
