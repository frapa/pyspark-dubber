from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName
from pyspark_dubber.sql.types import DataType


@incompatibility("options are completely ignored")
@sql_func(col_name_args="col")
def from_json(col: ColumnOrName, schema: DataType | str, options = None) -> Expr:
    return col.unwrap_as(schema)
