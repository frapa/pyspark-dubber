import sys
from pathlib import Path
from types import TracebackType
from typing import Type


class _PySparkReplacer:
    _path = str(Path(__file__).parent)

    def __call__(self) -> "_PySparkReplacer":
        return self.__enter__()

    def __enter__(self) -> "_PySparkReplacer":
        sys.path.insert(0, self._path)
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        sys.path.remove(self._path)


replace_pyspark = _PySparkReplacer()
