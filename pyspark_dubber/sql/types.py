import abc
import dataclasses
import re
from typing import Any

import ibis
import ibis.expr.datatypes
import lark
from duckdb.experimental.spark.sql.types import StructField

DDL_GRAMMAR = f"""
%import common.WS
%ignore WS

start: _type | _struct_fields
_type: struct | array | map | ATOMIC

struct: "struct"i "<" _struct_fields ">"
_struct_fields: struct_field ("," struct_field)*
struct_field: NAME _type

array: "array"i "<" _type ">"
map: "map"i "<" _type "," _type ">"

ATOMIC: "string"i | "timestamp"i | "date"i | "boolean"i | "binary"i | "decimal"i | "float"i | "double"i | "byte"i | "short"i | "int"i | "long"i 
NAME: /[A-Za-z_][A-Za-z0-9_]*/i
"""
ddl_parser = lark.Lark(DDL_GRAMMAR)


class _DDLTransformer(lark.Transformer):
    def start(self, args):
        if not args:
            raise ValueError("No types provided")
        if isinstance(args[0], StructField):
            return StructType(args)
        return args[0]

    def struct_field(self, args):
        return StructField(args[0].value, args[1])

    def struct(self, args):
        return StructType(args)

    def array(self, args):
        return ArrayType(args[0])

    def map(self, args):
        return MapType(args[0], args[1])

    def ATOMIC(self, ddl):
        subclass_list = DataType.__subclasses__()
        while subclass_list:
            subclass = subclass_list.pop()
            subclass_list.extend(subclass.__subclasses__())

            # Abstract class
            if abc.ABC in subclass.__bases__:
                continue

            if ddl.lower() in subclass._ddl_base_names():
                return subclass()

        raise ValueError(f"No DataType found for DDL: {ddl}")


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
            elif schema.is_binary():
                return BinaryType()
            elif schema.is_boolean():
                return BooleanType()
            elif schema.is_int8() or schema.is_uint8():
                return ByteType()
            elif schema.is_int16() or schema.is_uint16():
                return ShortType()
            elif schema.is_int32() or schema.is_uint32():
                return IntegerType()
            elif schema.is_int64() or schema.is_uint64():
                return LongType()
            elif schema.is_float32():
                return FloatType()
            elif schema.is_float64():
                return DoubleType()
            elif schema.is_decimal():
                return DecimalType(schema.precision, schema.scale)
            elif schema.is_date():
                return DateType()
            elif schema.is_timestamp():
                return TimestampType()
            elif schema.is_null():
                return NullType()
            elif schema.is_array():
                return ArrayType(DataType.from_ibis(schema.value_type), True)
            elif schema.is_map():
                return MapType(
                    DataType.from_ibis(schema.key_type),
                    DataType.from_ibis(schema.value_type),
                    True,
                )
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
    def fromDDL(ddl: str) -> "DataType":
        # TODO: Support nullability
        ddl = ddl.replace(":", "").strip()
        ast = ddl_parser.parse(ddl)
        res = _DDLTransformer().transform(ast)
        return res

    @abc.abstractmethod
    def to_ibis(self, nullable: bool = True) -> ibis.DataType: ...

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

    def __str__(self) -> str:
        return self.simpleString()


class AtomicType(DataType, abc.ABC): ...


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

    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.expr.datatypes.Struct.from_tuples(
            [
                (
                    f.name,
                    f.dataType.to_ibis(nullable=f.nullable),
                )
                for f in self.fields
            ],
            nullable=nullable,
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

    def to_ibis(self, nullable=True) -> ibis.DataType:
        elem_dtype = self.elementType.to_ibis(nullable=self.containsNull)
        return ibis.expr.datatypes.Array(elem_dtype, nullable=nullable)

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

    def to_ibis(self, nullable=True) -> ibis.DataType:
        key_dtype = self.keyType.to_ibis(nullable=False)
        val_dtype = self.valueType.to_ibis(nullable=self.valueContainsNull)
        return ibis.expr.datatypes.Map(key_dtype, val_dtype, nullable=nullable)

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
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("boolean", nullable=nullable)

    def _to_pyspark(self, st):
        return st.BooleanType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("boolean",)


@dataclasses.dataclass
class StringType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("string", nullable=nullable)

    def _to_pyspark(self, st):
        return st.StringType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("string",)


@dataclasses.dataclass
class CharType(StringType):
    length: int

    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("string", nullable=nullable)

    def simpleString(self) -> str:
        return f"char({self.length})"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return (f"char",)


@dataclasses.dataclass
class VarcharType(StringType):
    length: int

    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("string", nullable=nullable)

    def simpleString(self) -> str:
        return f"varchar({self.length})"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return (f"varchar",)


@dataclasses.dataclass
class BinaryType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("bytes", nullable=nullable)

    def _to_pyspark(self, st):
        return st.BinaryType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("binary",)


class NumericType(AtomicType, abc.ABC): ...


class IntegralType(NumericType, abc.ABC): ...


@dataclasses.dataclass
class ByteType(IntegralType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("int8", nullable=nullable)

    def _to_pyspark(self, st):
        return st.ByteType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "tinyint", "byte"


@dataclasses.dataclass
class ShortType(IntegralType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("int16", nullable=nullable)

    def _to_pyspark(self, st):
        return st.ShortType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "smallint", "short"


@dataclasses.dataclass
class IntegerType(IntegralType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("int32", nullable=nullable)

    def _to_pyspark(self, st):
        return st.IntegerType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "int", "integer"


@dataclasses.dataclass
class LongType(IntegralType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("int64", nullable=nullable)

    def _to_pyspark(self, st):
        return st.LongType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "bigint", "long"


class FractionalType(NumericType, abc.ABC): ...


@dataclasses.dataclass
class FloatType(FractionalType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("float32", nullable=nullable)

    def _to_pyspark(self, st):
        return st.FloatType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "float", "real"


@dataclasses.dataclass
class DoubleType(FractionalType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("float64", nullable=nullable)

    def _to_pyspark(self, st):
        return st.DoubleType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("double",)


@dataclasses.dataclass
class DecimalType(FractionalType):
    precision: int = 10
    scale: int = 0

    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype(f"decimal({self.precision}, {self.scale})", nullable=nullable)

    def _to_pyspark(self, st):
        return st.DecimalType(self.precision, self.scale)

    def simpleString(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("decimal", "dec", "numeric")


@dataclasses.dataclass
class DateType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("date", nullable=nullable)

    def _to_pyspark(self, st):
        return st.DateType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("date",)


@dataclasses.dataclass
class TimestampType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("timestamp", nullable=nullable)

    def _to_pyspark(self, st):
        return st.TimestampType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return "timestamp", "timestamp_ntz"


TimestampNTZType = TimestampType


@dataclasses.dataclass
class NullType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("null", nullable=nullable)

    def _to_pyspark(self, st):
        return st.NullType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return ("void",)


@dataclasses.dataclass
class DayTimeIntervalType(AtomicType):
    def to_ibis(self, nullable=True) -> ibis.DataType:
        return ibis.dtype("interval", nullable=nullable)

    def _to_pyspark(self, st):
        return st.DayTimeIntervalType()

    @staticmethod
    def _ddl_base_names() -> tuple[str, ...]:
        return (
            "interval year",
            "interval year to month",
            "interval month",
            "interval day",
            "interval day to hour",
            "interval day to minute",
            "interval day to second",
            "interval hour",
            "interval hour to minute",
            "interval hour to second",
            "interval minute",
            "interval minute to second",
            "interval second",
        )


YearMonthIntervalType = DayTimeIntervalType
CalendarIntervalType = DayTimeIntervalType
