# pyspark_dubber.sql.functions.randstr

```python
pyspark_dubber.sql.functions.randstr(
	length: pyspark_dubber.sql.expr.Expr | int,
	seed: pyspark_dubber.sql.expr.Expr | int | None = None,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.randstr.html)

!!! warning "Incompatibility Note"

    The `seed` argument is not honored. Output is lowercase-only.

