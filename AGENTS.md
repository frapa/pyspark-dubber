# AGENTS.md — PySpark Dubber

> Guide for AI coding agents contributing to this codebase.

## Mission

Bug-for-bug compatibility with the [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html), powered by [Ibis](https://ibis-project.org/) under the hood. When PySpark does something surprising, we do the same surprising thing. PySpark behavior **is** the specification. Project docs: https://frapa.github.io/pyspark-dubber/

## Architecture

```
pyspark_dubber/
├── docs.py                    # @incompatibility decorator
├── replace_pyspark.py         # Context manager for transparent import replacement
├── errors/base.py             # PySpark exception hierarchy
└── sql/
    ├── session.py             # SparkSession (builder pattern, createDataFrame)
    ├── dataframe.py           # DataFrame wrapping ibis.Table
    ├── expr.py                # Expr/WhenExpr wrapping ibis Value/Deferred
    ├── grouped_data.py        # GroupedData for aggregations
    ├── row.py                 # PySpark Row implementation
    ├── input.py               # DataFrameReader (CSV, JSON, Parquet)
    ├── output.py              # SparkOutput (write formats)
    ├── types.py               # Type system with Lark DDL parser
    └── functions/
        ├── __init__.py        # Re-exports all functions + __all__
        ├── _helper.py         # sql_func decorator
        ├── normal.py          # col, lit, expr, ColumnOrName
        ├── aggregate.py       # sum, avg, count, min, max, ...
        ├── array.py           # array operations
        ├── collection.py      # size, element_at, filter, transform
        ├── conditional.py     # when, coalesce
        ├── hash.py            # sha2, md5, ...
        ├── json.py            # from_json, to_json
        ├── math.py            # abs, ceil, floor, round, sqrt, ...
        ├── misc.py            # broadcast, assert_true, ...
        ├── predicate.py       # isnull, isnotnull, isnan
        ├── sort.py            # asc, desc, nulls ordering
        ├── string.py          # concat, substring, trim, regex, ...
        ├── struct.py          # struct operations
        └── temporal.py        # date/time functions
```

Function modules mirror the [PySpark SQL functions docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) — each section maps to a file.

## Three Cardinal Rules

### 1. Use `@sql_func` for every simple function wrapper

If your function takes column arguments and returns an Ibis expression, it **must** use `@sql_func`. This decorator handles column-name-to-Ibis conversion, auto-aliasing, and argument formatting. Do not manually call `col_fn()` / `.to_ibis()` / `.alias()` when `@sql_func` can do it for you.

```python
# CORRECT
@sql_func(col_name_args="col")
def upper(col: ColumnOrName) -> Expr:
    return col.upper()

# WRONG — manual wiring that @sql_func already handles
def upper(col: ColumnOrName) -> Expr:
    return Expr(col_fn(col).to_ibis().upper()).alias(f"upper({col})")
```

### 2. Never implement stubs that throw `NotImplementedError`

Do not add method signatures with `raise NotImplementedError()`. Stubs appear as "implemented" in the auto-generated docs and mislead users. If you cannot implement a method, leave it out entirely.

### 3. PySpark behavior is the specification

When in doubt about behavior (argument types, edge cases, null handling, error messages), run it in real PySpark and match. The `@comparison_test` decorator exists exactly for this: it runs the same code against PySpark and pyspark-dubber and asserts identical output.

## Adding a New Function — Checklist

1. **Identify the category** — check which section the function belongs to in the [PySpark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html).
2. **Choose the module** — place it in the corresponding file under `pyspark_dubber/sql/functions/`.
3. **Implement with `@sql_func`** — use `col_name_args` for all parameters that accept `ColumnOrName`.
4. **Type the signature** — use `ColumnOrName` for column-or-string params. Use `Expr | float` (not `ColumnOrName`) for params that are literal values, not column names. See [Type System](#type-system-quick-reference).
5. **Export** — add the function to both the `import` block and `__all__` list in `pyspark_dubber/sql/functions/__init__.py`.
6. **Add aliases** — if PySpark has aliases (e.g., `mean = avg`), add them as simple assignments after the function.
7. **Write tests** — add tests in the corresponding file under `tests/funtions/` (yes, "funtions" — intentional directory name).
8. **Handle incompatibilities** — if an Ibis backend limitation prevents exact PySpark behavior, add `@incompatibility("description")` **above** `@sql_func`.
9. **Run tests** — `pytest tests/funtions/test_<category>.py -v`.

## Type System Quick Reference

These types are defined in `pyspark_dubber/sql/expr.py` and `pyspark_dubber/sql/functions/normal.py`:

| Type | Meaning | Use for |
|------|---------|---------|
| `ColumnOrName` = `Expr \| str` | A column reference — strings are column **names** | Function params where a string means "look up this column" |
| `Expr \| float` | A column or literal value | Params like `percentage` in `percentile()` where a string would NOT be a column name |
| `ScalarValue` | `str \| int \| float \| bool \| date \| datetime` | Single literal values |
| `LiteralValue` | `ScalarValue \| list[ScalarValue]` | Literal values including arrays |
| `Literal["any", "all"]` | Python `typing.Literal` | Constrained string params (e.g., `how` in `dropna`) |
| `Sequence[str]` | Ordered collection | Prefer over `List[Any]` for param types |

**The #1 mistake**: Using `ColumnOrName` for a parameter that takes a literal string value. `ColumnOrName` means "if you pass a string, it's a column name." If the parameter is a literal value (like `errMsg` in `assert_true`), use `Expr | str` instead and wrap with `lit()`.

## Canonical Code Patterns

### Simple `@sql_func` (single column)
```python
# pyspark_dubber/sql/functions/aggregate.py
@sql_func(col_name_args="col")
def avg(col: ColumnOrName) -> Expr:
    return col.mean()
```

### Multi-argument `@sql_func`
```python
@sql_func(col_name_args=("col1", "col2"))
def corr(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.corr(col2)
```

### Function alias
```python
mean = avg
percentile_approx = approx_percentile
std = stddev
```

### Manual Expr construction (when `@sql_func` doesn't fit)

Use when special logic is needed (e.g., `count("*")` handling):

```python
# pyspark_dubber/sql/functions/aggregate.py
def count(col: ColumnOrName) -> Expr:
    if col == "*":
        return Expr(ibis.deferred.count()).alias("count(1)")
    return Expr(col_fn(col).to_ibis().count()).alias(f"count({col})")
```

### `@incompatibility` + `@sql_func` combo

`@incompatibility` goes **above** `@sql_func`:

```python
# pyspark_dubber/sql/functions/aggregate.py
@incompatibility("The frequency argument is not honored.")
@sql_func(col_name_args="col")
def percentile(
    col: ColumnOrName,
    percentage: Expr | float | Sequence[float],
    frequency: Expr | int = 1,
) -> Expr:
    percentage = lit(percentage).to_ibis()
    return col.quantile(percentage)
```

### Higher-order functions (lambdas)
```python
# pyspark_dubber/sql/functions/collection.py
@sql_func(col_name_args="col")
def filter(col: ColumnOrName, f: UnaryOrBinary) -> Expr:
    if len(inspect.signature(f).parameters) == 1:
        ibis_func = lambda v: f(Expr(v)).to_ibis()
    else:
        ibis_func = lambda v, i: f(Expr(v), Expr(i)).to_ibis()
    return col.filter(ibis_func)
```

### Custom UDFs (when Ibis has no equivalent)
```python
# pyspark_dubber/sql/functions/string.py
def base64(col: ColumnOrName) -> Expr:
    @ibis.udf.scalar.python
    def _base64_encode(data: bytes) -> str:
        return base64_lib.b64encode(data).decode()

    col_bin = col_fn(col).to_ibis().cast("binary")
    return Expr(_base64_encode(col_bin)).alias(f"base64({col})")
```

Prefer `@ibis.udf.scalar.pyarrow` over `@ibis.udf.scalar.python` when the operation maps to a PyArrow compute function (see the `_trim` pattern in `string.py`).

## Testing

### The `@comparison_test` decorator

Defined in `tests/conftest.py`. It runs the same test function against real PySpark and pyspark-dubber, then asserts both produce identical `stdout` and identical `toPandas()` results.

```python
# tests/funtions/test_aggregate.py
@parametrize(
    avg={"func": lambda f: f.avg("num")},
    sum={"func": lambda f: f.sum("num")},
)
@comparison_test
def test_agg(spark, load, func) -> None:
    functions = load("sql.functions")
    df = spark.createDataFrame(
        [("a", 1), ("b", 2)], ["group", "num"]
    )
    df.groupby("group").agg(func(functions)).orderBy("group").show()
```

Key points:
- `load("sql.functions")` dynamically imports either `pyspark.sql.functions` or `pyspark_dubber.sql.functions`.
- `spark` is either a real `SparkSession` or a `DubberSparkSession`.
- `@parametrize` goes **above** `@comparison_test`.
- Test directory is `tests/funtions/` (not `tests/functions/`).

### Running tests

```bash
pip install -e ".[dev]"
pytest tests/                              # all tests
pytest tests/funtions/test_aggregate.py -v # single category
pytest tests/test_pyspark_scripts.py -v    # PySpark example scripts
```

## Common Pitfalls

1. **Forgetting `@sql_func`** — manually doing `col_fn(col).to_ibis()` + `.alias()` when the decorator handles it.
2. **`ColumnOrName` vs literal `str`** — `ColumnOrName` means strings become column lookups. For literal string params, use `Expr | str` and wrap with `lit()`.
3. **Wrong decorator order** — `@incompatibility` must be above `@sql_func`. Reversed order breaks both.
4. **Not exporting** — new functions must be added to both the `import` block and `__all__` in `functions/__init__.py`.
5. **Implementing stubs** — `raise NotImplementedError()` makes functions appear implemented in docs. Just omit them.
6. **Wrong test directory** — it's `tests/funtions/`, not `tests/functions/`.
7. **Python 3.10 compatibility** — no f-string nested quotes (PEP 701 is 3.12+), no `match` on complex types without fallback.
8. **Forgetting `lit()` wrappers** — non-column literal params (`percentage`, `frequency`) need `lit(value).to_ibis()`.
9. **Not matching PySpark alias format** — alias strings must match PySpark's `.show()` column headers exactly (e.g., `"count(1)"` not `"count(*)"` for `count("*")`).
10. **Returning wrong type** — function return type should be `Expr`, not `ibis.Value` or raw expressions.

## Build & CI

```bash
pip install -e ".[dev]"       # Install with dev dependencies
pytest tests/                  # Run all tests
python docs/generate.py        # Regenerate API reference docs
```

- **Build system**: Hatchling (`pyproject.toml`)
- **Python**: 3.10+
- **CI workflows**: `tests-and-pypi.yml` (test matrix 3.10-3.12, PyPI publish on tags), `generate-docs.yml` (auto-generate docs), `publish-gh-pages.yml` (deploy docs)

## Scope

PySpark SQL module only: `DataFrame`, `Column`/`Expr`, `functions`, `types`, `SparkSession`, `Row`, I/O.

**Out of scope**: RDD API, MLlib, GraphFrames, Structured Streaming, SparkContext.
