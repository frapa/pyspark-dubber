import sys
from pathlib import Path
from types import TracebackType
from typing import Type

import pyspark_dubber as pyspark
from pyspark_dubber import sql
from pyspark_dubber.sql import functions, types


class _PySparkReplacer:
    _path = str(Path(__file__).parent)

    def __call__(self) -> "_PySparkReplacer":
        return self.__enter__()

    def __enter__(self) -> "_PySparkReplacer":
        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.sql"] = sql
        sys.modules["pyspark.sql.functions"] = functions
        sys.modules["pyspark.sql.types"] = types
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        sys.path.remove(self._path)


replace_pyspark = _PySparkReplacer()
