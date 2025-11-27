from pyspark_dubber.sql.column import Column

ColumnOrName = Column | str


def col(col: str) -> Column:
    return Column()
