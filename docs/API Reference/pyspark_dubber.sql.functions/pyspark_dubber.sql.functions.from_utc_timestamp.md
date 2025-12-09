# pyspark_dubber.sql.functions.from_utc_timestamp

```python
pyspark_dubber.sql.functions.from_utc_timestamp(
	timestamp: pyspark_dubber.sql.expr.Expr | str,
	tz: pyspark_dubber.sql.expr.Expr | str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_utc_timestamp.html)

!!! warning "Incompatibility Note"

    Currently the `tz` timezone argument is ignored, therefore this function is mostly useless.

