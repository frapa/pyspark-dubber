# DataFrame.orderBy

```python
DataFrame.orderBy(
	*cols: pyspark_dubber.sql.expr.Expr | str,
	ascending: bool | list[bool] = True,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)

!!! warning "Incompatibility Note"

    Sorting by column ordinals (which are 1-based, not 0-based) is not supported yet. Additionally, this function still needs better testing around edge cases, when sorting with complex column expressions which include sorting.

