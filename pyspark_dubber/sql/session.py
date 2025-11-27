from typing import Iterable, Any

import ibis
import pandas
import numpy

from pyspark_dubber.errors import PySparkTypeError, PySparkValueError
from pyspark_dubber.sql.row import Row
from pyspark_dubber.sql.dataframe import DataFrame
from pyspark_dubber.sql.types import (
    StructType,
    AtomicType,
    StructField,
    StringType,
    IntegerType, DataType,
)


class _Builder:
    def master(self, master: str) -> "_Builder":
        return self

    def appName(self, app_name: str) -> "_Builder":
        return self

    def getOrCreate(self) -> "SparkSession":
        return SparkSession()


class SparkConfig:
    # TODO: default spark configuration
    _conf = {}

    def get(self, key: str) -> Any:
        return self._conf.get(key)

    def set(self, key: str, value: Any) -> None:
        self._conf[key] = value


class SparkSession:
    builder = _Builder()
    conf = SparkConfig()

    def createDataFrame(
        self,
        # TODO: RDD support
        data: Iterable[Row | dict[str, Any] | Any] | pandas.DataFrame | numpy.ndarray,
        schema: StructType | AtomicType | str | None = None,
        samplingRatio: float | None = None,
        verifySchema: bool = True,
    ) -> DataFrame:
        if isinstance(data, numpy.ndarray):
            raise NotImplementedError("Numpy ndarray support is not implemented yet.")

        data_for_schema = data
        if isinstance(data, pandas.DataFrame):
            data_for_schema = data.to_dict(orient="records")
            if schema is None:
                data = data.convert_dtypes()  # Otherwise pandas says "object" for anything
                schema = StructType([
                    StructField(c, DataType.from_pandas(str(t)), True)
                    for c, t in zip(data.columns, data.dtypes)
                ])

        if schema is None:
            schema = self._infer_schema(data_for_schema)
        elif verifySchema:
            self._verify_schema(data_for_schema, schema)

        return DataFrame(ibis.memtable(data, columns=[f.name for f in schema.fields]), schema)

    def _infer_schema(self, data: Iterable[Row | dict[str, Any] | Any]) -> StructType:
        data = list(data)
        if not data:
            raise PySparkValueError(
                "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from empty dataset."
            )

        fields = None
        for row in data[:100]:
            if not isinstance(row, (Row, dict, list, tuple)):
                raise PySparkTypeError(
                    f"[CANNOT_INFER_SCHEMA_FOR_TYPE] Can not infer schema for type: `{type(row).__name__}`."
                )

            if not fields:
                fields = [None] * len(row)

            for i, value in enumerate(row):
                if fields[i] is None:
                    if isinstance(value, str):
                        fields[i] = StructField(f"_{i+1}", StringType(), True)
                    elif isinstance(value, int):
                        fields[i] = StructField(f"_{i+1}", IntegerType(), True)
                    else:
                        raise NotImplementedError(
                            f"Type not implemented yet: {type(value).__name__}"
                        )

            if None not in fields:
                break

        if None in fields:
            raise PySparkValueError(
                "[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring."
            )

        return StructType(fields)

    def _verify_schema(
        self,
        data: Iterable[Row | dict[str, Any] | Any],
        schema: StructType | AtomicType | str,
    ) -> None:
        if isinstance(schema, str):
            raise NotImplementedError("DDL schema support is not implemented yet.")

        for row in data:
            if isinstance(row, dict):
                row = Row(**row)
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
                if i > len(row):
                    raise PySparkValueError(
                        f"[MISSING_FIELD] Missing field: {field.name}."
                    )

                value = row.get(field.name, row[keys[i]])
                if isinstance(value, str) and not isinstance(field.dataType, StringType):
                    raise PySparkTypeError(f"Type mismatch: {field.dataType} != {StringType()}.")

            # TODO: extra fields?
