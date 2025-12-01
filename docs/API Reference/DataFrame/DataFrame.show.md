# DataFrame.show

```python
DataFrame.show(
	n: int = 20,
	truncate: bool | int = True,
	vertical: bool = False,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)

!!! warning "Incompatibility Note"

    The `truncate` and `vertical` parameters are not honored. Additionally, the output is not printed justified exactly as pyspark as of the current version.

    #### Example pyspark output
    ```text
    +----------+---+
    |First Name|Age|
    +----------+---+
    |     Scott| 50|
    |      Jeff| 45|
    |    Thomas| 54|
    |       Ann| 34|
    +----------+---+
    ```
    #### Example pyspark-dubber output
    ```text
    +----------+---+
    |First Name|Age|
    +----------+---+
    |Scott     |50 |
    |Jeff      |45 |
    |Thomas    |54 |
    |Ann       |34 |
    +----------+---+
    ```


