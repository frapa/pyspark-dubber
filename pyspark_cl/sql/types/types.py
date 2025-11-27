import abc
import dataclasses
from typing import Any


class DataType(abc.ABC):
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

    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.StructType([
            st.StructField(f.name, f.dataType.to_pyspark(), f.nullable, f.metadata)
            for f in self.fields
        ])


@dataclasses.dataclass
class StringType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.StringType()


@dataclasses.dataclass
class IntegerType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.IntegerType()


@dataclasses.dataclass
class LongType(AtomicType):
    def to_pyspark(self):
        try:
            from pyspark.sql import types as st
        except ImportError as err:
            raise ImportError("pyspark must be installed separately to use .to_spark()") from err

        return st.LongType()
