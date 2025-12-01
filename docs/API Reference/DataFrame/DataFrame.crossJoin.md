# DataFrame.crossJoin

```python
DataFrame.crossJoin(
	other: 'DataFrame',
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crossJoin.html)

!!! warning "Incompatibility Note"

    pyspark allows duplicate column names, and by default does not prefix/suffix the columns of the other dataframe at all. Our backend (ibis) currently does not support duplicate column names, so this function suffixes all columns on other with '_right'.

