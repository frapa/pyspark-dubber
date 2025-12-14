# SparkOutput.csv

```python
SparkOutput.csv(
	path: str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.csv.html)

!!! warning "Incompatibility Note"

    Most parameters are unsupported, the writing of files cannot be reproduced 1:1because it depends on spark internals such as partitions.

