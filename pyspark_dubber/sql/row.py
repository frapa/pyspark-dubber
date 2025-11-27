from typing import Any


class Row:
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def asDict(self) -> dict[str, Any]:
        return self._kwargs

    def __getitem__(self, key: str) -> Any:
        return self._kwargs[key]

    def __contains__(self, key: str) -> bool:
        return key in self._kwargs

    def __str__(self) -> str:
        fields = ", ".join(
            f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}"
            for k, v in self._kwargs.items()
        )
        return f"Row({fields})"

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: "Row") -> bool:
        # Make it compatible with pyspark Row objects, so they can be compared!
        return self.asDict() == other.asDict()

    def __len__(self) -> int:
        return len(self._kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return self._kwargs.get(key, default)
