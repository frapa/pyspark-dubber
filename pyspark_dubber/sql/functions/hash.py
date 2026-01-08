from typing import Literal, Callable
from hashlib import md5, sha1, sha256, sha512

import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName


def _hash(
    how: Literal["md5", "sha1", "sha256", "sha512"],
) -> Callable[[ColumnOrName], Expr]:
    @sql_func(col_name_args="col")
    def _hash_internal(col: ColumnOrName) -> Expr:
        @ibis.udf.scalar.python
        def _hash_python(data: str) -> bytes:
            if how == "md5":
                return md5(data.encode()).digest()
            elif how == "sha1":
                return sha1(data.encode()).digest()
            elif how == "sha256":
                return sha256(data.encode()).digest()
            elif how == "sha512":
                return sha512(data.encode()).digest()

            raise NotImplementedError(f"Hashing algorithm {how} is not supported.")

        return _hash_python(col)

    return _hash_internal


md5 = _hash("md5")
sha1 = _hash("sha1")
sha = sha1


@incompatibility("Only `numBits` 256 or 512 are supported.")
def sha2(col: ColumnOrName, numBits: int) -> Expr:
    if numBits == 0 or numBits == 256:
        return _hash("sha256")(col)
    elif numBits == 512:
        return _hash("sha512")(col)
    else:
        raise ValueError("Only numBits 256 or 512 are supported.")
