# DataFrame.dropna

```python
DataFrame.dropna(
	how: str = typing.Literal['any', 'all'],
	thresh: int | None = None,
	subset: str | Sequence[str] | None = None,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropna.html)

!!! warning "Incompatibility Note"

    The `thresh` parameter is not honored.

