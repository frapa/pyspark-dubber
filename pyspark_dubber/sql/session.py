from datetime import date, datetime
from itertools import count
from typing import Iterable, Any, Sequence

import ibis
import numpy
import pandas
import pyarrow

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.errors import PySparkTypeError, PySparkValueError
from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.input import DataFrameReader
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.types import (
    StructType,
    AtomicType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    DataType,
    DateType,
    TimestampType,
    ArrayType,
)


class SparkConfig:
    # TODO: default spark configuration
    _conf = {}

    def get(self, key: str) -> Any:
        return self._conf.get(key)

    def set(self, key: str, value: Any) -> None:
        self._conf[key] = value


class SparkSession:
    conf = SparkConfig()
    read = DataFrameReader()

    class Builder:
        def master(self, master: str) -> "_Builder":
            return self

        def appName(self, app_name: str) -> "_Builder":
            return self

        def getOrCreate(self) -> "SparkSession":
            return SparkSession()

    builder = Builder()

    @incompatibility(
        "Generally `createDataFrame` is a complex method, so certain edge "
        "cases are not handled correctly. Some notable incompatibilities with pyspark:\n\n"
        "- numpy arrays are not yet accepted as input data type.\n"
        "- `samplingRatio` is not honored.\n"
    )
    def createDataFrame(
        self,
        # TODO: RDD support
        data: Iterable[Row | dict[str, Any] | Any] | pandas.DataFrame | numpy.ndarray,
        schema: StructType | AtomicType | str | Sequence[str] | None = None,
        samplingRatio: float | None = None,
        verifySchema: bool = True,
    ) -> DataFrame:
        if isinstance(data, numpy.ndarray):
            raise NotImplementedError("Numpy ndarray support is not implemented yet.")

        if isinstance(schema, str):
            schema = DataType.fromDDL(schema)

        data_for_schema = data
        if isinstance(data, pandas.DataFrame):
            # Keep schema column names if they differ from pandas df name
            if schema is not None:
                data.columns = schema.names
            data_for_schema = data.to_dict(orient="records")

        # Ibis implements all this but we re-implement a first pass to raise
        # the same errors as pyspark for error-level compatibility
        if schema is None or isinstance(schema, Sequence):
            final_schema = self._infer_schema(data_for_schema, schema)
        elif verifySchema:
            self._verify_schema(data_for_schema, schema)
            final_schema = schema

        ibis_struct = final_schema.to_ibis()
        ibis_schema = ibis.Schema.from_tuples(ibis_struct.fields.items())
        # Convert to pyarrow, because pandas is casting types weirdly (like int to float if there's a null)
        if isinstance(data, pandas.DataFrame):
            return DataFrame(ibis.memtable(data, schema=ibis_schema))
        elif isinstance(data, numpy.ndarray):
            raise NotImplementedError("Numpy ndarray support is not implemented yet.")
        else:
            if len(data) > 0 and isinstance(data[0], dict):
                arrow_data = pyarrow.Table.from_pylist(
                    data, schema=ibis_schema.to_pyarrow()
                )
            elif len(data) > 0 and isinstance(data[0], Row):
                arrow_data = pyarrow.Table.from_pylist(
                    [r.asDict() for r in data], schema=ibis_schema.to_pyarrow()
                )
            else:
                arrow_data = pyarrow.Table.from_pylist(
                    [dict(zip(ibis_struct.names, r)) for r in data],
                    schema=ibis_schema.to_pyarrow(),
                )
            return DataFrame(ibis.memtable(arrow_data, schema=ibis_schema))

    def _infer_schema(
        self,
        data: Iterable[Row | dict[str, Any] | Any],
        preferred_column_names: list[str] | None = None,
    ) -> StructType:
        if preferred_column_names is None:
            preferred_column_names = (f"_{i}" for i in count(1))

        data = list(data)
        if not data:
            raise PySparkValueError(
                "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from empty dataset."
            )

        fields = None
        for row in data[:100]:
            if isinstance(row, Row):
                dict_row = row.asDict()
            elif isinstance(row, dict):
                dict_row = row
            elif isinstance(row, (list, tuple)):
                # Name columns as _1, _2, etc.
                dict_row = dict(zip(preferred_column_names, row))
            else:
                raise PySparkTypeError(
                    f"[CANNOT_INFER_SCHEMA_FOR_TYPE] Can not infer schema for type: `{type(row).__name__}`."
                )

            if not fields:
                fields = [None] * len(row)

            for i, (col, value) in enumerate(dict_row.items()):
                if fields[i] is None:
                    inferred_type = self._infer_type(value)
                    if inferred_type is None:
                        continue
                    fields[i] = StructField(col, inferred_type, True)

            if None not in fields:
                break

        if None in fields:
            raise PySparkValueError(
                "[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring."
            )

        return StructType(fields)

    def _infer_type(self, value: Any) -> DataType | None:
        """Infer the DataType for a given value."""
        if value is None:
            return None
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, datetime):
            return TimestampType()
        elif isinstance(value, date):
            return DateType()
        elif isinstance(value, list):
            # Infer element type from first non-null element
            element_type = None
            for elem in value:
                element_type = self._infer_type(elem)
                if element_type is not None:
                    break
            if element_type is None:
                raise PySparkTypeError(
                    f"Type error. Could no determine the type of {value}"
                )
            return ArrayType(element_type, True)
        else:
            raise NotImplementedError(
                f"Type not implemented yet: {type(value).__name__}"
            )

    def _verify_schema(
        self,
        data: Iterable[Row | dict[str, Any] | Any],
        schema: StructType | AtomicType | str,
    ) -> None:
        for row in data:
            if isinstance(row, dict):
                row = Row(**row)
            elif isinstance(row, Sequence):
                if isinstance(schema, AtomicType):
                    raise PySparkTypeError(
                        f"[CANNOT_ACCEPT_OBJECT_IN_TYPE] `{type(schema).__name__}` "
                        f"can not accept object `{row}` in type `{type(row).__name__}`."
                    )
                row = Row(**dict(zip(schema.names, row)))
            elif not isinstance(row, Row):
                # Atomic type
                if not isinstance(schema, AtomicType):
                    raise PySparkTypeError(
                        f"[CANNOT_ACCEPT_OBJECT_IN_TYPE] `{type(schema).__name__}` "
                        f"can not accept object `{row}` in type `{type(row).__name__}`."
                    )

            if isinstance(schema, AtomicType):
                raise NotImplementedError("AtomicType support is not implemented yet.")

            keys = list(row.asDict().keys())
            for i, field in enumerate(schema.fields):
                if i >= len(row):
                    raise PySparkValueError(
                        f"[MISSING_FIELD] Missing field: {field.name}."
                    )

                value = row.get(field.name, row[keys[i]])
                if isinstance(value, str) and not isinstance(
                    field.dataType, StringType
                ):
                    raise PySparkTypeError(
                        f"Type mismatch for field '{field.name}': {field.dataType} != {StringType()}."
                    )

            # TODO: extra fields?

    def stop(self) -> None:
        pass
