# SparkOutput.parquet

```python
SparkOutput.parquet(
	path: str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html)

!!! warning "Incompatibility Note"

    Most parameters are unsupported, the writing of files cannot be reproduced 1:1because it depends on spark internals such as partitions.

