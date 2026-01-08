import dataclasses
import math
from typing import Sequence, Literal, Any

import ibis
import ibis.expr.operations
import pandas

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.errors import AnalysisException
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions import expr, col
from pyspark_dubber.sql.functions.normal import ColumnOrName
from pyspark_dubber.sql.output import SparkOutput
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.types import ArrayType
from pyspark_dubber.sql.types import StructType, DataType


@dataclasses.dataclass
class DataFrame:
    _ibis_df: ibis.Table

    @property
    def columns(self) -> list[str]:
        return list(self._ibis_df.columns)

    @property
    def schema(self) -> StructType:
        return DataType.from_ibis(self._ibis_df.schema)

    @property
    def dtypes(self) -> list[tuple[str, str]]:
        # TODO: does this work for nested types?
        return [
            (f.name, f.dataType.simpleString())
            for f in DataType.from_ibis(self._ibis_df.schema()).fields
        ]

    @property
    def write(self) -> SparkOutput:
        return SparkOutput(self._ibis_df)

    @incompatibility("The `level` parameter is not honored.")
    def printSchema(self, level: int | None = None) -> None:
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
        "+----------+---+\n"
        "|First Name|Age|\n"
        "+----------+---+\n"
        "|     Scott| 50|\n"
        "|      Jeff| 45|\n"
        "|    Thomas| 54|\n"
        "|       Ann| 34|\n"
        "+----------+---+\n"
        "```\n"
        "#### Example pyspark-dubber output\n"
        "```text\n"
        "+----------+---+\n"
        "|First Name|Age|\n"
        "+----------+---+\n"
        "|Scott     |50 |\n"
        "|Jeff      |45 |\n"
        "|Thomas    |54 |\n"
        "|Ann       |34 |\n"
        "+----------+---+\n"
        "```\n"
    )
    def show(
        self, n: int = 20, truncate: bool | int = True, vertical: bool = False
    ) -> None:
        schema = DataType.from_ibis(self._ibis_df.schema())

        header = [f.name for f in schema.fields]
        justification: list[Literal["<", ">"]] = [">" for _ in header]
        rows = []
        lengths = [len(h) for h in header]
        for row in self._ibis_df.limit(n).to_pyarrow().to_pylist(maps_as_pydicts="strict"):
            cells = [_format_value(c) for c in row.values()]
            rows.append(cells)
            lengths = [max(lengths[i], len(c), 3) for i, c in enumerate(cells)]

        divider = "+" + "+".join("-" * l for l in lengths) + "+"

        print(divider)
        header_str = "|".join(
            _format_cell(h, l, j) for h, l, j in zip(header, lengths, justification)
        )
        print(f"|{header_str}|")

        print(divider)
        for cells in rows:
            cell_str = "|".join(
                _format_cell(c, l, j) for c, l, j in zip(cells, lengths, justification)
            )
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
            e if isinstance(e, str) else str(id(e)): col(e).to_ibis()
            for c in cols
            for e in (self._ibis_df.columns if isinstance(c, str) and c == "*" else [c])
        }
        return DataFrame(self._ibis_df.select(*cols.values()))

    def withColumn(self, colName: str, col: Expr) -> "DataFrame":
        return self.withColumns({colName: col})

    def withColumns(self, colsMap: dict[str, Expr]) -> "DataFrame":
        return DataFrame(
            self._ibis_df.mutate(**{n: e.to_ibis() for n, e in colsMap.items()})
        )

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        return self.withColumnsRenamed({existing: new})

    def withColumnsRenamed(self, colsMap: dict[str, str]) -> "DataFrame":
        # Ibis does renaming with the map in the reverse direction (new -> old)
        # than spark does (old -> new)
        return DataFrame(
            self._ibis_df.rename({new: existing for existing, new in colsMap.items()})
        )

    def filter(self, condition: Expr | str) -> "DataFrame":
        if isinstance(condition, str):
            condition = expr(condition)
        return DataFrame(self._ibis_df.filter(condition.to_ibis()))

    where = filter

    def limit(self, num: int) -> "DataFrame":
        return DataFrame(self._ibis_df.limit(num))

    def offset(self, num: int) -> "DataFrame":
        return DataFrame(self._ibis_df.limit(0, offset=num))

    @incompatibility(
        "Sorting by column ordinals (which are 1-based, not 0-based) is not supported yet. "
        "Additionally, this function still needs better testing around edge cases, "
        "when sorting with complex column expressions which include sorting."
    )
    def orderBy(
        self, *cols: ColumnOrName, ascending: bool | list[bool] = True
    ) -> "DataFrame":

        if isinstance(ascending, bool):
            ascending = [ascending] * len(cols)

        sorted_ibis_exprs = []
        for c, asc in zip(cols, ascending):
            ibis_expr = col(c).to_ibis()
            # TODO: test edge case when the column is an expression with a specific sort order
            #   (for example col("my_col").desc()) but the ascending parameter is also set for
            #   the column. Which one takes precedence in pyspark?
            # Here, we ignore the flag if the column is already sorted
            # TODO: this only works if the sorting is the last operation on the column
            if isinstance(
                ibis_expr.resolve(self._ibis_df).op(), ibis.expr.operations.SortKey
            ):
                sorted_ibis_exprs.append(ibis_expr)
            else:
                sorted_ibis_exprs.append(ibis_expr.asc() if asc else ibis_expr.desc())

        return DataFrame(self._ibis_df.order_by(*sorted_ibis_exprs))

    sort = orderBy
    sortWithinPartitions = orderBy

    def union(self, other: "DataFrame") -> "DataFrame":
        if len(self.columns) != len(other.columns):
            raise ValueError("Cannot union dataframes with different column counts.")
        elif [t for _, t in self.dtypes] != [t for _, t in other.dtypes]:
            raise ValueError(
                f"Cannot union dataframes with different dtypes. {self.dtypes} != {other.dtypes}"
            )

        other_aligned = other.select(
            *(col(o).alias(c) for c, o in zip(self.columns, other.columns))
        )
        return self.unionByName(other_aligned)

    unionAll = union

    def unionByName(
        self, other: "DataFrame", allowMissingColumns: bool = False
    ) -> "DataFrame":
        my_cols = set(self._ibis_df.columns)
        other_cols = set(other._ibis_df.columns)
        my_missing_cols = other_cols.difference(my_cols)
        other_missing_cols = my_cols.difference(other_cols)

        if allowMissingColumns:
            me_filled = self._ibis_df.mutate(
                **{c: ibis.null(other._ibis_df.schema()[c]) for c in my_missing_cols}
            )

            other_filled = other._ibis_df.mutate(
                **{c: ibis.null(self._ibis_df.schema()[c]) for c in other_missing_cols}
            )

            return DataFrame(me_filled).unionByName(DataFrame(other_filled))

        if other_missing_cols:
            cols_fmt = ", ".join(other_cols)
            raise AnalysisException(
                f'Cannot resolve column name "{other_missing_cols.pop()}" among ({cols_fmt}).'
            )
        if my_missing_cols:
            cols_fmt = ", ".join(my_cols)
            raise AnalysisException(
                f'Cannot resolve column name "{my_missing_cols.pop()}" among ({cols_fmt}).'
            )

        return DataFrame(self._ibis_df.union(other._ibis_df))

    @incompatibility(
        "Currently only column names are supported for grouping, "
        "column expressions are not supported."
    )
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
        elif on is None:
            # Spark does a cross-join when on=None (and it's not documented)
            return self.crossJoin(other)

        result = self._ibis_df.join(
            other._ibis_df, predicates=[col(e).to_ibis() for e in on], how=how
        )
        return DataFrame(result)

    @incompatibility(
        "pyspark allows duplicate column names, and by default does not prefix/suffix "
        "the columns of the other dataframe at all. Our backend (ibis) currently does not "
        "support duplicate column names, so this function suffixes all columns on other with '_right'."
    )
    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        return DataFrame(self._ibis_df.cross_join(other._ibis_df))

    def collect(self) -> list[Row]:
        return [Row(**d) for d in self._ibis_df.to_pyarrow().to_pylist(maps_as_pydicts="strict")]

    def first(self) -> Row | None:
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def take(self, num: int) -> list[Row]:
        return self.limit(num).collect()

    def head(self, n: int = 1) -> list[Row] | Row:
        if n == 1:
            return self.first()
        return self.take(n)

    def tail(self, num: int) -> list[Row]:
        return self.offset(self.count() - num).collect()

    def cache(self) -> "DataFrame":
        return DataFrame(self._ibis_df.cache())

    def count(self) -> int:
        return self._ibis_df.count().to_pandas()

    def agg(self, *exprs: Expr | dict[str, Expr]) -> "DataFrame":
        if not exprs:
            raise ValueError("agg() requires at least one argument")
        elif isinstance(exprs[0], dict):
            if len(exprs) > 1:
                raise ValueError("agg() does not support multiple dict arguments")
            exprs = [e.alias(n) for n, e in exprs[0].items()]

        return DataFrame(self._ibis_df.agg([e.to_ibis() for e in exprs]))

    def alias(self, alias: str) -> "DataFrame":
        return DataFrame(self._ibis_df.alias(alias))

    @incompatibility(
        "Our backend (ibis) does not support duplicate column names like pyspark, "
        "therefore this function does not support dropping columns with the same name. "
        "You cannot anyway currently create such dataframes using `pyspark-dubber`."
    )
    def drop(self, *cols: ColumnOrName) -> "DataFrame":
        cols = [
            (
                c
                if isinstance(c, str)
                else _resolve_ibis_expr(c.to_ibis(), self._ibis_df).get_name()
            )
            for c in cols
        ]
        return DataFrame(self._ibis_df.drop(*cols))

    def dropDuplicates(self, subset: Sequence[str] | None = None) -> "DataFrame":
        return DataFrame(self._ibis_df.distinct(on=subset))

    drop_duplicates = dropDuplicates

    def distinct(self) -> "DataFrame":
        return self.dropDuplicates()

    def isEmpty(self) -> bool:
        return self.count() == 0

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

    @incompatibility("The `thresh` parameter is not honored.")
    def dropna(
        self,
        how: str = Literal["any", "all"],
        thresh: int | None = None,
        subset: str | Sequence[str] | None = None,
    ) -> "DataFrame":
        return DataFrame(self._ibis_df.drop_null(subset, how=how))

    def isLocal(self):
        return True

    def checkpoint(self, eager: bool = True) -> "DataFrame":
        return self

    def coalesce(self, numPartitions: int) -> "DataFrame":
        return self

    def repartition(self, numPartitions: int, *cols: ColumnOrName) -> "DataFrame":
        return self

    def repartitionByRange(
        self, numPartitions: int, *cols: ColumnOrName
    ) -> "DataFrame":
        return self

    def createGlobalTempView(self, name: str) -> bool:
        return True

    createOrReplaceGlobalTempView = createGlobalTempView

    def registerTempTable(self, name: str) -> bool:
        return True

    def createTempView(self, name: str) -> bool:
        return True

    createOrReplaceTempView = createTempView

    def persist(self, storageLevel) -> "DataFrame":
        return self

    def __getitem__(self, name: str) -> Expr:
        if name not in self._ibis_df.columns:
            raise ValueError(f"Column {name} does not exist")
        return Expr(self._ibis_df[name])

    def __getattr__(self, name: str) -> Expr:
        return self[name]

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


def _resolve_ibis_expr(
    expr: ibis.Value | ibis.Deferred, table: ibis.Table
) -> ibis.Value:
    if isinstance(expr, ibis.Deferred):
        return expr.resolve(table)
    return expr


def _format_cell(value: str, length: int, justification: Literal["<", ">"]) -> str:
    if justification == "<":
        return f"{value:<{length}}"
    return f"{_format_value(value):>{length}}"


def _format_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, bytes):
        hex_val = value.hex().upper()
        bytes_list = " ".join(hex_val[i : i + 2] for i in range(0, len(hex_val), 2))
        return f"[{bytes_list}]"
    if isinstance(value, list):
        # Format arrays like PySpark: [a, b, c] instead of ['a', 'b', 'c']
        # Recursively format elements to handle nested arrays
        formatted_items = [_format_value(item) for item in value]
        joined = ", ".join(formatted_items)
        return f"[{joined}]"
    if isinstance(value, dict):
        return f"""{{{', '.join(_format_value(v) for k, v in value.items())}}}"""
    return str(value)
