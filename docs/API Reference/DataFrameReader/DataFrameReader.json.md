# DataFrameReader.json

```python
DataFrameReader.json(
	path: str | pathlib.Path | list[str | pathlib.Path],
	schema: str | pyspark_dubber.sql.types.StructType | None = None,
	*args,
	**kwargs,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html)

!!! warning "Incompatibility Note"

    Most parameters are accepted but completely ignored.

    The path cannot be a RDD like in pyspark for now. Schema is only supported as a StructType, DLL strings are unsupported.

