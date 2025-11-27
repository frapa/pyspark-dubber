import abc
import dataclasses
from typing import Any

import pandas


class DataType(abc.ABC):
    @abc.abstractmethod
    def to_pyspark(self): ...

    @abc.abstractmethod
    def __str__(self) -> str: ...

    @abc.abstractmethod
    def simpleString(self) -> str: ...

    @staticmethod
    def from_pandas(dtype: str) -> "DataType":
        if dtype == "string":
            return StringType()
        if dtype == "Int64":
            return LongType()

        raise NotImplementedError(f"Pandas conversion not implemented for type: {dtype}")


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

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.StructType([
            st.StructField(f.name, f.dataType.to_pyspark(), f.nullable, f.metadata)
            for f in self.fields
        ])

    def __str__(self) -> str:
        return "struct"

    def simpleString(self) -> str:
        return "struct"


@dataclasses.dataclass
class StringType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.StringType()

    def __str__(self) -> str:
        return "string"

    def simpleString(self) -> str:
        return "string"


@dataclasses.dataclass
class IntegerType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.IntegerType()

    def __str__(self) -> str:
        return "integer"

    def simpleString(self) -> str:
        return "int"


@dataclasses.dataclass
class LongType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.LongType()

    def __str__(self) -> str:
        return "long"

    def simpleString(self) -> str:
        return "bigint"
