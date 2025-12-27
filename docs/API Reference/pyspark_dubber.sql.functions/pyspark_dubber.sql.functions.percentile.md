# pyspark_dubber.sql.functions.percentile

```python
pyspark_dubber.sql.functions.percentile(
	col: pyspark_dubber.sql.expr.Expr | str,
	percentage: pyspark_dubber.sql.expr.Expr | float | Sequence[float],
	frequency: pyspark_dubber.sql.expr.Expr | int = 1,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile.html)

!!! warning "Incompatibility Note"

    The frequency argument is not honored.

