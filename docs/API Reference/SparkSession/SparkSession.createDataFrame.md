# SparkSession.createDataFrame

```python
SparkSession.createDataFrame(
	data: Union[Iterable[pyspark_dubber.sql.row.Row | dict[str, Any] | Any], pandas.core.frame.DataFrame, numpy.ndarray],
	schema: Union[pyspark_dubber.sql.types.StructType, pyspark_dubber.sql.types.AtomicType, str, Sequence[str], NoneType] = None,
	samplingRatio: float | None = None,
	verifySchema: bool = True,
)
```

[PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html)

!!! warning "Incompatibility Note"

    Generally `createDataFrame` is a complex method, so certain edge cases are not handled correctly. Some notable incompatibilities with pyspark:

    - numpy arrays are not yet accepted as input data type.
    - `samplingRatio` is not honored.


