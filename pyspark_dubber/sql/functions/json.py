import json
import sys
from typing import Any

import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName
from pyspark_dubber.sql.types import DataType, StructType




@incompatibility("options are completely ignored")
@sql_func(col_name_args="col", do_not_print_args=("schema", "options"))
def from_json(col: ColumnOrName, schema: DataType | str, options = None) -> Expr:
    if isinstance(schema, str):
        schema = DataType.fromDDL(schema)

    ret_type = schema.to_ibis()

    def _struct_default(data: dict[str, Any], dtype: StructType) -> dict[str, Any]:
        for f in dtype.fields:
            if isinstance(f.dataType, StructType):
               data[f.name] = _struct_default(data.get(f.name, {}), f.dataType)
            elif f.name not in data:
                # TODO: zero type for non nullable or error?
                data[f.name] = None if f.nullable else 0


        return data

    @ibis.udf.scalar.python
    def _parse(data: str) -> ret_type:
        if isinstance(schema, StructType):
            try:
                return _struct_default(json.loads(data), schema)
            except json.JSONDecodeError as err:
                print(f"Invalid JSON: '{data}'", file=sys.stderr)
                return None

        try:
            return json.loads(data)
        except json.JSONDecodeError as err:
            print(f"Invalid JSON: '{data}'", file=sys.stderr)
            return None

    return _parse(col)
