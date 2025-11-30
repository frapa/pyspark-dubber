from collections.abc import Callable


def incompatibility(docs: str) -> Callable[[Callable], Callable]:
    def _decorator(func: Callable) -> Callable:
        func.__incompatibility_docs__ = docs
        return func

    return _decorator
