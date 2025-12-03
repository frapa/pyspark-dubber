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

    def simpleString(self) -> str:
        return self._ddl_base_names()[0]

    @staticmethod
    @abc.abstractmethod
    def _ddl_base_names() -> tuple[str, ...]: ...

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

    @staticmethod
    def from_DDL(ddl: str) -> "DataType":
        ddl = ddl.replace(":", "")

        # TODO: support more types
        subclass_list = DataType.__subclasses__()
        while subclass_list:
            subclass = subclass_list.pop()
            subclass_list.extend(subclass.__subclasses__())

            # Abstract class
            if subclass.__name__ == "AtomicType":
                continue

            if ddl in subclass._ddl_base_names():
                return subclass()

        raise ValueError(f"No DataType found for DDL: {ddl}")

    @abc.abstractmethod
    def to_ibis(self) -> ibis.DataType: ...

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st

            return self._to_pyspark(st)
        except ImportError as err:
            raise ImportError(
                "pyspark must be installed separately to use .to_spark()"
            ) from err

    @abc.abstractmethod
    def _to_pyspark(self, st): ...


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

    def _to_pyspark(self, st):
        return st.StructType(
            [
                st.StructField(f.name, f.dataType.to_pyspark(), f.nullable, f.metadata)
                for f in self.fields
            ]
        )

    def simpleString(self) -> str:
        fields = ", ".join(
            f"f{f.name} {f.dataType.simpleString()}" for f in self.fields
        )
        return f"struct<{fields}>"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("struct",)


@dataclasses.dataclass
class ArrayType(DataType):
    elementType: DataType
    containsNull: bool = True

    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.Array(self.elementType.to_ibis())

    def _to_pyspark(self, st):
        return st.ArrayType(self.elementType.to_pyspark(), self.containsNull)

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("array",)


@dataclasses.dataclass
class MapType(DataType):
    keyType: DataType
    valueType: DataType
    valueContainsNull: bool = True

    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.Map(self.keyType.to_ibis(), self.valueType.to_ibis())

    def _to_pyspark(self, st):
        return st.MapType(
            self.keyType.to_pyspark(),
            self.valueType.to_pyspark(),
            self.valueContainsNull,
        )

    def simpleString(self) -> str:
        return f"map<{self.keyType.simpleString()}, {self.valueType.simpleString()}>"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("map",)


@dataclasses.dataclass
class BooleanType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.boolean

    def _to_pyspark(self, st):
        return st.BooleanType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("boolean",)


@dataclasses.dataclass
class StringType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.string

    def _to_pyspark(self, st):
        return st.StringType()

    def __str__(self) -> str:
        return "string"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("string",)


@dataclasses.dataclass
class ByteType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int8

    def _to_pyspark(self, st):
        return st.ByteType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "tinyint", "byte"


@dataclasses.dataclass
class ShortType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int16

    def _to_pyspark(self, st):
        return st.ShortType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "smallint", "short"


@dataclasses.dataclass
class IntegerType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int32

    def _to_pyspark(self, st):
        return st.IntegerType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "int", "integer"


@dataclasses.dataclass
class LongType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.int64

    def _to_pyspark(self, st):
        return st.LongType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "bigint", "long"


@dataclasses.dataclass
class FloatType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.float32

    def _to_pyspark(self, st):
        return st.FloatType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "float", "real"


@dataclasses.dataclass
class DoubleType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.float64

    def _to_pyspark(self, st):
        return st.DoubleType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("double",)


@dataclasses.dataclass
class DateType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.date

    def _to_pyspark(self, st):
        return st.DateType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("date",)


@dataclasses.dataclass
class TimestampType(AtomicType):
    def to_ibis(self) -> ibis.DataType:
        return ibis.expr.datatypes.timestamp

    def _to_pyspark(self, st):
        return st.TimestampType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "timestamp", "timestamp_ntz"
