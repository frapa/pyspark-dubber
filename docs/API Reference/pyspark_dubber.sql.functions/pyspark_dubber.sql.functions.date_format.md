# pyspark_dubber.sql.functions.date_format

```python
pyspark_dubber.sql.functions.date_format(
	date: pyspark_dubber.sql.expr.Expr | str,
	format: str,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html)

!!! warning "Incompatibility Note"

    Certain esoteric formatting options are not supported, such as:

    - G for the era designator
    - Q for quarter of year
    - timezones might be formatted differently or be offset by your local timezone, since pyspark seems to assume UTC.

