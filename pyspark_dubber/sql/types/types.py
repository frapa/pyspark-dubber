import abc
import dataclasses
from typing import Any

import ibis
import ibis.expr.datatypes
from duckdb.experimental.spark.sql.types import StructField


class DataType(abc.ABC):
    # Ugly, but this is what pyspark does
    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    @abc.abstractmethod
    def simpleString(self) -> str: ...

    @staticmethod
    def from_ibis(schema: ibis.Schema | ibis.DataType) -> "StructType":
        if isinstance(schema, ibis.DataType):
            if schema.is_string():
                return StringType()
            elif schema.is_int32() or schema.is_uint32():
                return IntegerType()
            elif schema.is_int64() or schema.is_uint64():
                return LongType()
            elif schema.is_float32():
                return FloatType()
            elif schema.is_float64():
                return DoubleType()
            elif schema.is_array():
                return ArrayType(DataType.from_ibis(schema.value_type), True)
            elif schema.is_struct():
                return StructType(
                    [
                        StructField(name, DataType.from_ibis(typ), typ.nullable)
                        for name, typ in zip(schema.names, schema.types)
                    ]
                )
            else:
                raise NotImplementedError(
                    f"Ibis schema conversion not implemented for type: {schema}"
                )

        return StructType(
            [
                StructField(name, DataType.from_ibis(typ), typ.nullable)
                for name, typ in schema.fields.items()
            ]
        )

    @abc.abstractmethod
    def to_ibis(self) -> ibis.DataType: ...

    @abc.abstractmethod
    def to_pyspark(self): ...


class AtomicType(DataType, abc.ABC):
    pass


@dataclasses.dataclass
class StructField:
    name: str
    dataType: DataType
    nullable: bool = True
    metadata: dict[str, Any] | None = None


@dataclasses.dataclass
class StructType(DataType):
    fields: list[StructField]

    @property
    def names(self) -> list[str]:
        return list(f.name for f in self.fields)

    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.Struct.from_tuples(
            [(f.name, f.dataType.to_ibis()) for f in self.fields]
        )

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.StructType(
            [
                st.StructField(f.name, f.dataType.to_pyspark(), f.nullable, f.metadata)
                for f in self.fields
            ]
        )

    def simpleString(self) -> str:
        return "struct"


@dataclasses.dataclass
class ArrayType(DataType):
    elementType: DataType
    containsNull: bool = True

    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.Array(self.elementType.to_ibis())

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.ArrayType(self.elementType.to_pyspark(), self.containsNull)

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"


@dataclasses.dataclass
class StringType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.string

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.StringType()

    def __str__(self) -> str:
        return "string"

    def simpleString(self) -> str:
        return "string"


@dataclasses.dataclass
class IntegerType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int32

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.IntegerType()

    def simpleString(self) -> str:
        return "int"


@dataclasses.dataclass
class LongType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int64

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.LongType()

    def simpleString(self) -> str:
        return "bigint"


@dataclasses.dataclass
class FloatType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.float32

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.FloatType()

    def simpleString(self) -> str:
        return "float"


@dataclasses.dataclass
class DoubleType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.float64

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

        return st.DoubleType()

    def simpleString(self) -> str:
        return "double"
