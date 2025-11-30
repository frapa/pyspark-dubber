# Overview

`pyspark-dubber` goal is bug-for-bug compatibility with `pyspark`,
therefore it exposes the exact same API, and the
[pyspark API reference](https://spark.apache.org/docs/latest/api/python/reference/index.html)
is a good starting point.

The goal of the `pyspark-dubber` API reference
is to document the know differences with the pyspark, which are usually
due to incomplete implementation or cases where it is impossible to
achieve the same behavior as in pyspark.

If there is an undocumented deviation from the pyspark API,
be it interface or behavior, it should be considered a bug
(at a minimum a documentation bug) and
[reported here](https://github.com/frapa/pyspark-dubber/issues).