# pyspark_dubber.sql.functions.to_binary

```python
pyspark_dubber.sql.functions.to_binary(
	col: pyspark_dubber.sql.expr.Expr | str,
	format: pyspark_dubber.sql.expr.Expr | str = Expr(_ibis_expr='hex'),
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_binary.html)

!!! warning "Incompatibility Note"

    Currently only works for utf-8. We couldn't figure out pyspark algorithm yet, it's well documented.

