# DataFrame.groupBy

```python
DataFrame.groupBy(
	*cols: pyspark_dubber.sql.expr.Expr | str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)

!!! warning "Incompatibility Note"

    Currently only column names are supported for grouping, column expressions are not supported.

