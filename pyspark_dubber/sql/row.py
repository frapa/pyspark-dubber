from typing import Any


class Row:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def asDict(self) -> dict[str, Any]:
        return self.kwargs

    def __getitem__(self, key: str) -> Any:
        return self.kwargs[key]

    def __str__(self) -> str:
        fields = ", ".join(
            f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}"
            for k, v in self.kwargs.items()
        )
        return f"Row({fields})"

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: "Row") -> bool:
        # Make it compatible with pyspark Row objects, so they can be compared!
        return self.asDict() == other.asDict()
