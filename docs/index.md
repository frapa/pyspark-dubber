# PySpark Dubber

A compatibility layer for the `pyspark` API, allowing you to run `pyspark` code on backends
such as DuckDB and Polars without porting your code.

## Why

Lately, SQL engines and DataFrame libraries such as DuckDB and Polars have become popular,
offering great performance for non-distributed analytical workflows up to relatively large
datasets (tens of GBs). For these sizes and below, Spark adds a lot of overhead and its startup
time is relatively slow, making it not very cost- and time-efficient.

However, Spark is still the most mature and widely used data processing framework,
meaning that many people and organizations have large codebases relying on its APIs.

`pyspark-dubber` is a library that allows you to run `pyspark` code on many backends, such as
DuckDB and Polars (actually any backend supported by [ibis](https://ibis-project.org/) at this time),
making it possible to migrate old code to a new backend with minimal changes.

The aspiration of `pyspark-dubber` is to be bug-for-bug compatible with `pyspark`.

## Documentation

You can find API documentation and more information about the project in our
[documentation page](https://frapa.github.io/pyspark-dubber/).

## Principles

- Try to be as close as possible as the behavior of `pyspark`.
- Prefer efficient implementation (backend-native > pyarrow UDF > python UDF),
  but if necessary for use slow code to ensure compatibility. This might
  negate the benefits of using this library to run on other backends if the
  code uses many esoteric functions. However, for the most common cases,
  we expect most normal code to rely on backend-native and fast code,
  while UDF polyfills are there to fill the gaps and allow the code to
  complete correctly.
