import dataclasses
from typing import Sequence, Literal

import ibis
import pandas

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions.base import ColumnOrName
from pyspark_dubber.sql.output import SparkOutput
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.types import StructType, DataType
from pyspark_dubber.sql.types.types import ArrayType


@dataclasses.dataclass
class DataFrame:
    _ibis_df: ibis.Table

    @property
    def write(self) -> SparkOutput:
        return SparkOutput(self._ibis_df)

    def printSchema(self) -> None:
        schema = DataType.from_ibis(self._ibis_df.schema())
        print("root")
        _print_struct_or_array(schema)
        print()

    @incompatibility(
        "The `truncate` and `vertical` parameters are not honored. "
        "Additionally, the output is not printed justified exactly as pyspark "
        "as of the current version.\n\n"
        "#### Example pyspark output\n"
        "```text\n"
        '+----------+---+\n'
        '|First Name|Age|\n'
        '+----------+---+\n'
        '|     Scott| 50|\n'
        '|      Jeff| 45|\n'
        '|    Thomas| 54|\n'
        '|       Ann| 34|\n'
        '+----------+---+\n'
        "```\n"
        "#### Example pyspark-dubber output\n"
        "```text\n"
        '+----------+---+\n'
        '|First Name|Age|\n'
        '+----------+---+\n'
        '|Scott     |50 |\n'
        '|Jeff      |45 |\n'
        '|Thomas    |54 |\n'
        '|Ann       |34 |\n'
        '+----------+---+\n'
        "```\n"
    )
    def show(
        self, n: int = 20, truncate: bool | int = True, vertical: bool = False
    ) -> None:
        schema = DataType.from_ibis(self._ibis_df.schema())

        header = [f.name for f in schema.fields]
        rows = []
        lengths = [len(h) for h in header]
        for row in self._ibis_df.limit(n).to_pandas().to_dict(orient="records"):
            cells = [str(v) for v in row.values()]
            rows.append(cells)
            lengths = [max(lengths[i], len(c)) for i, c in enumerate(cells)]

        divider = "+" + "+".join("-" * l for l in lengths) + "+"

        print(divider)
        header_str = "|".join(f"{h:<{l}}" for h, l in zip(header, lengths))
        print(f"|{header_str}|")

        print(divider)
        for cells in rows:
            cell_str = "|".join(f"{c:<{l}}" for c, l in zip(cells, lengths))
            print(f"|{cell_str}|")

        print(divider)
        print()

    def __repr__(self) -> str:
        schema = DataType.from_ibis(self._ibis_df.schema())
        fields = ", ".join(
            f"{f.name}: {f.dataType.simpleString()}" for f in schema.fields
        )
        return f"DataFrame[{fields}]"

    def select(self, *cols: ColumnOrName) -> "DataFrame":
        # TODO: does this work when selecting expressions that define new columns?
        # Use dict for ordering and for and automatic duplicate removal
        cols = {
            e if isinstance(e, str) else str(id(e)): _col_expr_to_ibis(e)
            for c in cols
            for e in (self._ibis_df.columns if isinstance(c, str) and c == "*" else [c])
        }
        return DataFrame(self._ibis_df.select(*cols.values()))

    def withColumn(self, colName: str, col: Expr) -> "DataFrame":
        return DataFrame(self._ibis_df.mutate(**{colName: col.to_ibis()}))

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        return DataFrame(self._ibis_df.rename({new: existing}))

    @incompatibility("Using a string as a SQL expressions is not supported yet.")
    def filter(self, condition: Expr | str) -> "DataFrame":
        # TODO: this is important but ibis does not seem to have a way to do this
        if isinstance(condition, str):
            raise NotImplementedError(
                "SQL expressions are not yet supported in filter yet."
            )
        return DataFrame(self._ibis_df.filter(_col_expr_to_ibis(condition)))

    def limit(self, num: int) -> "DataFrame":
        return DataFrame(self._ibis_df.limit(num))

    def orderBy(self, *cols: ColumnOrName) -> "DataFrame":
        return DataFrame(self._ibis_df.order_by(*[_col_expr_to_ibis(c) for c in cols]))

    def unionByName(
        self, other: "DataFrame", allowMissingColumns: bool = False
    ) -> "DataFrame":
        if allowMissingColumns:
            my_cols = set(self._ibis_df.columns)
            other_cols = set(other._ibis_df.columns)

            my_missing_cols = other_cols.difference(my_cols)
            me_filled = self._ibis_df.mutate(
                **{c: ibis.null(other._ibis_df.schema()[c]) for c in my_missing_cols}
            )

            other_missing_cols = my_cols.difference(other_cols)
            other_filled = other._ibis_df.mutate(
                **{c: ibis.null(self._ibis_df.schema()[c]) for c in other_missing_cols}
            )

            return DataFrame(me_filled).unionByName(DataFrame(other_filled))

        return DataFrame(self._ibis_df.union(other._ibis_df))

    @incompatibility("Currently only column names are supported for grouping, "
                     "column expressions are not supported.")
    def groupBy(self, *cols: ColumnOrName) -> "GroupedData":
        # To avoid circular imports
        from pyspark_dubber.sql.grouped_data import GroupedData

        # TODO: column expressions
        return GroupedData(self._ibis_df.group_by(*cols))

    groupby = groupBy

    def join(
        self,
        other: "DataFrame",
        on: str | Sequence[str] | ColumnOrName | None = None,
        how: Literal[
            "inner",
            "cross",
            "outer",
            "full",
            "fullouter",
            "full_outer",
            "left",
            "leftouter",
            "left_outer",
            "right",
            "rightouter",
            "right_outer",
            "semi",
            "leftsemi",
            "left_semi",
            "anti",
            "leftanti",
            "left_anti",
        ] = "inner",
    ) -> "DataFrame":
        if isinstance(on, (str, Expr)):
            on = [on]
        result = self._ibis_df.join(
            other._ibis_df, predicates=[_col_expr_to_ibis(e) for e in on], how=how
        )
        return DataFrame(result)

    def collect(self) -> list[Row]:
        return [Row(**d) for d in self._ibis_df.to_pandas().to_dict(orient="records")]

    def cache(self) -> "DataFrame":
        return DataFrame(self._ibis_df.cache())

    def count(self) -> int:
        return self._ibis_df.count().to_pandas()

    def fillna(
        self,
        value: int | float | str | bool | dict[str, int | float | str | bool],
        subset: str | Sequence[str] = None,
    ) -> "DataFrame":
        # TODO: test if value is of one type and subset lists a column that doesn't have that type,
        #   for example value="123" and the subset column is an integer. Spark ignores the column.
        if subset is not None:
            if isinstance(value, dict):
                value = {k: v for k, v in value.items() if k in subset}
            else:
                value = {k: value for k in subset}
        return DataFrame(self._ibis_df.fill_null(value))

    def __getitem__(self, name: str) -> Expr:
        if name not in self._ibis_df.columns:
            raise ValueError(f"Column {name} does not exist")
        return Expr(self._ibis_df[name])

    def toPandas(self) -> pandas.DataFrame:
        return self._ibis_df.to_pandas()


def _print_struct_or_array(typ: StructType | ArrayType, indent: str = "") -> None:
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


def _col_expr_to_ibis(col: ColumnOrName) -> ibis.Value | ibis.Deferred | str:
    if isinstance(col, str):
        return col

    return col.to_ibis()
