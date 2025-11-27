from pyspark_dubber.sql import Column

ColumnOrName = Column | str


def col(col: str) -> Column:
    return Column()
