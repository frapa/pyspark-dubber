# pyspark_dubber.sql.functions.sort_array

```python
pyspark_dubber.sql.functions.sort_array(
	col: pyspark_dubber.sql.expr.Expr | str,
	asc: bool = True,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sort_array.html)

!!! warning "Incompatibility Note"

    Descending sort (asc=False) is not supported. Arrays are always sorted in ascending order.

