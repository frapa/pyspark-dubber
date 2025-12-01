# DataFrame.drop

```python
DataFrame.drop(
	*cols: pyspark_dubber.sql.expr.Expr | str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html)

!!! warning "Incompatibility Note"

    Our backend (ibis) does not support duplicate column names like pyspark, therefore this function does not support dropping columns with the same name. You cannot anyway currently create such dataframes using `pyspark-dubber`.

