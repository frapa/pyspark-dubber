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
    +-----+-----+
    |phase|count|
    +-----+-----+
    |0.5  |3288 |
    |1.0  |34129|
    |2.0  |52868|
    |3.0  |27110|
    |4.0  |9294 |
    +-----+-----+
    ```
    #### Example pyspark-dubber output
    ```text
    +-----+-----+
    |phase|count|
    +-----+-----+
    |  0.5| 3288|
    |  1.0|34129|
    |  2.0|52868|
    |  3.0|27110|
    |  4.0| 9294|
    +-----+-----+
    ```


