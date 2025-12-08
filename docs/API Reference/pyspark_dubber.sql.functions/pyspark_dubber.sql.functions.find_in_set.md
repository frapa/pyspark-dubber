# pyspark_dubber.sql.functions.find_in_set

```python
pyspark_dubber.sql.functions.find_in_set(
	str: str,
	strarray: pyspark_dubber.sql.expr.Expr | str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.find_in_set.html)

!!! warning "Incompatibility Note"

    find_in_set only supports strings as the first argument, not dynamically another column like in pyspark.

