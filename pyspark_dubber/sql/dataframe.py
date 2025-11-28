import dataclasses

import ibis
import ibis.common.selectors
import pandas

from pyspark_dubber.sql.column import RefColumn
from pyspark_dubber.sql.functions.base import ColumnOrName
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.types import StructType, DataType
from pyspark_dubber.sql.types.types import ArrayType


@dataclasses.dataclass
class DataFrame:
    _ibis_df: ibis.Table

    def collect(self) -> list[Row]:
        return [Row(**d) for d in self._ibis_df.to_pandas().to_dict(orient="records")]

    # TODO: more tests
    def show(
        self, n: int = 20, truncate: bool | int = True, vertical: bool = False
    ) -> None:
        schema = DataType.from_ibis(self._ibis_df.schema())

        header = [f.name for f in schema.fields]
        rows = []
        lengths = [len(h) for h in header]
        for row in self._ibis_df.to_pandas().to_dict(orient="records"):
            cells = [str(v) for v in row.values()]
            rows.append(cells)
            lengths = [max(lengths[i], len(c)) for i, c in enumerate(cells)]

        divider = "+" + "+".join("-" * l for l in lengths) + "+"

        print(divider)
        header_str = "|".join(f"{h:>{l}}" for h, l in zip(header, lengths))
        print(f"|{header_str}|")

        print(divider)
        for cells in rows:
            cell_str = "|".join(f"{c:>{l}}" for c, l in zip(cells, lengths))
            print(f"|{cell_str}|")

        print(divider)
        print()

    # TODO: more tests
    def printSchema(self) -> None:
        schema = DataType.from_ibis(self._ibis_df.schema())
        print("root")
        _print_struct_or_array(schema)
        print()

    def __repr__(self) -> str:
        schema = DataType.from_ibis(self._ibis_df.schema())
        fields = ", ".join(
            f"{f.name}: {f.dataType.simpleString()}" for f in schema.fields
        )
        return f"DataFrame[{fields}]"

    def toPandas(self) -> pandas.DataFrame:
        return self._ibis_df.to_pandas()

    def select(self, *cols: ColumnOrName) -> "DataFrame":
        # Use dict for ordering and for and automatic duplicate removal
        cols = {
            e: None for c in cols for e in (self._ibis_df.columns if c == "*" else [c])
        }
        return DataFrame(self._ibis_df.select(*cols.keys()))

    def groupBy(self, *cols: ColumnOrName) -> "GroupedData":
        # To avoid circular imports
        from pyspark_dubber.sql.grouped_data import GroupedData

        # TODO: column expressions
        return GroupedData(self._ibis_df.group_by(*cols))

    def groupby(self, *cols: ColumnOrName) -> "GroupedData":
        return self.groupBy(*[_col_expr_to_ibis(c) for c in cols])

    def orderBy(self, *cols: ColumnOrName) -> "DataFrame":
        return DataFrame(self._ibis_df.order_by(*[_col_expr_to_ibis(c) for c in cols]))

    def count(self) -> int:
        return self._ibis_df.count().to_pandas()


def _print_struct_or_array(typ: StructType | ArrayType, indent:str= "") -> None:
    if isinstance(typ, ArrayType):
        print(
            f"{indent} |-- element: {typ.elementType.typeName()} "
            f"(containsNull = {str(typ.containsNull).lower()})"
        )
        if isinstance(typ.elementType, (ArrayType, StructType)):
            _print_struct_or_array(typ.elementType, indent=f"{indent} |   ")
    elif isinstance(typ, StructType):
        for f in typ.fields:
            print(
                f"{indent} |-- {f.name}: {f.dataType.typeName()} (nullable = {str(f.nullable).lower()})"
            )
            if isinstance(f.dataType, (ArrayType, StructType)):
                _print_struct_or_array(f.dataType, indent=f"{indent} |   ")



def _col_expr_to_ibis(col: ColumnOrName) -> ibis.common.selectors.Selector | str:
    if isinstance(col, str):
        return col
    if isinstance(col, RefColumn):
        return col.ref

    raise NotImplementedError(
        f"pyspark API to ibis expression conversion not yet implemented for: {col}"
    )
