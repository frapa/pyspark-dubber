# pyspark_dubber.sql.functions.percentile_approx

```python
pyspark_dubber.sql.functions.percentile_approx(
	col: pyspark_dubber.sql.expr.Expr | str,
	percentage: pyspark_dubber.sql.expr.Expr | float | Sequence[float],
	accuracy: pyspark_dubber.sql.expr.Expr | int = 10000,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html)

!!! warning "Incompatibility Note"

    The accuracy argument is not honored.

