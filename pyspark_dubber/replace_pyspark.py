import sys
from types import TracebackType
from typing import Type

import pyspark_dubber as pyspark
from pyspark_dubber import sql
from pyspark_dubber.sql import functions, types


class _PySparkReplacer:
    def __init__(self) -> None:
        self._old_modules = {}

    def __call__(self) -> "_PySparkReplacer":
        return self.__enter__()

    def __enter__(self) -> "_PySparkReplacer":
        self._old_modules["pyspark"] = sys.modules.get("pyspark")
        self._old_modules["pyspark.sql"] = sys.modules.get("pyspark.sql")
        self._old_modules["pyspark.sql.functions"] = sys.modules.get(
            "pyspark.sql.functions"
        )
        self._old_modules["pyspark.sql.types"] = sys.modules.get("pyspark.sql.types")

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
        # Restore previous modules. This allows reuse of the same spark session in tests.
        for name, module in self._old_modules.items():
            sys.modules[name] = module


replace_pyspark = _PySparkReplacer()
