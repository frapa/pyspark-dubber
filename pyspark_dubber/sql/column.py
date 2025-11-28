import dataclasses


class Column:
    pass


@dataclasses.dataclass
class RefColumn(Column):
    ref: str
