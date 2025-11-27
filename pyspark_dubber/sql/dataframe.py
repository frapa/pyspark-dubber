import dataclasses

import ibis
import pandas
from pandas.io.sas.sas_constants import header_size_length, header_size_offset
from pyspark.cloudpickle import cell_set

from pyspark_dubber.sql.functions.base import ColumnOrName
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.types import StructType


@dataclasses.dataclass
class DataFrame:
    _ibis_df: ibis.Table
    _schema: StructType

    def collect(self) -> list[Row]:
        return [Row(**d) for d in self._ibis_df.to_pandas().to_dict(orient="records")]

    # TODO: more tests
    def show(self) -> None:
        header = [f.name for f in self._schema.fields]
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
        print("root")
        _print_struct(self._schema)
        print()

    def __repr__(self) -> str:
        fields = ", ".join(f"{f.name}: {f.dataType.simpleString()}" for f in self._schema.fields)
        return f"DataFrame[{fields}]"

    def toPandas(self) -> pandas.DataFrame:
        return self._ibis_df.to_pandas()

    def select(self, *cols: ColumnOrName) -> "DataFrame":
        # Use dict for ordering and for and automatic duplicate removal
        cols = {e: None for c in cols for e in (self._ibis_df.columns if c== "*" else [c])}
        return DataFrame(self._ibis_df.select(*cols.keys()), self._schema)


def _print_struct(struct: StructType) -> None:
    for f in struct.fields:
        print(f" |-- {f.name}: {f.dataType} (nullable = {str(f.nullable).lower()})")
        # TODO: test recursive
        if isinstance(f.dataType, StructType):
            _print_struct(f.dataType)
