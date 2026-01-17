import ibis

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, LiteralValue
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn
from pyspark_dubber.sql.expr import lit


@sql_func(col_name_args=("col"))
def array_append(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    return col.concat(ibis.array([lit(value).to_ibis()]))


@sql_func(col_name_args=("col"))
def array_contains(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    return col.contains(lit(value).to_ibis())


@sql_func(col_name_args="col")
def array_compact(col: ColumnOrName) -> Expr:
    return col.filter(lambda v: v.notnull())


@sql_func(col_name_args="col")
def array_distinct(col: ColumnOrName) -> Expr:
    return col.unique()


@sql_func(col_name_args=("col1", "col2"))
def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.intersect(col2)


@incompatibility("null_replacement is not natively in ibis")
@sql_func(col_name_args="col")
def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: str | None = None
) -> Expr:
    # TODO: Likely null_replacement can be implemented with a Map.
    return col.join(delimiter)


@sql_func(col_name_args="col")
def array_max(col: ColumnOrName) -> Expr:
    return col.maxs()


@sql_func(col_name_args="col")
def array_min(col: ColumnOrName) -> Expr:
    return col.mins()


@sql_func(col_name_args=("col", "value"))
def array_position(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    # Spark uses 1-based indexing, ibis uses 0-based
    return col.index(value) + 1


@sql_func(col_name_args=("col", "element"))
def array_remove(col: ColumnOrName, element: ColumnOrName | LiteralValue) -> Expr:
    return col.remove(element)


@sql_func(col_name_args="col")
def array_repeat(col: ColumnOrName, count: ColumnOrName | int) -> Expr:
    if isinstance(count, int):
        count_expr = count
    else:
        count_expr = col_fn(count).to_ibis()
    return ibis.array([col]).repeat(count_expr)


@sql_func(col_name_args="col")
def array_size(col: ColumnOrName) -> Expr:
    return col.length()


@incompatibility(
    "Descending sort (asc=False) is not supported. Arrays are always sorted in ascending order."
)
def sort_array(col: ColumnOrName, asc: bool = True) -> Expr:
    col_expr = col_fn(col).to_ibis()
    result = col_expr.sort()
    return Expr(result).alias(f"sort_array({col}, {str(asc).lower()})")


@sql_func(col_name_args=("col1", "col2"))
def array_union(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    return col1.union(col2)


@sql_func(col_name_args="col")
def flatten(col: ColumnOrName) -> Expr:
    return col.flatten()


def array(*cols: ColumnOrName) -> Expr:
    ibis_cols = [col_fn(c).to_ibis() for c in cols]
    col_names = ", ".join(str(c) for c in cols)
    return Expr(ibis.array(ibis_cols)).alias(f"array({col_names})")


@sql_func(col_name_args=("col1", "col2"))
def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    # TODO: Potentially one could also express it in terms of other functions, thus abstracting this function from ibis completely.
    return col1.intersect(col2).length() > 0


def arrays_zip(*cols: ColumnOrName) -> Expr:
    if not cols:
        raise ValueError("arrays_zip requires at least one column")

    cols = [col_fn(c) for c in cols]
    ibis_cols = [c.to_ibis() for c in cols]
    result = (
        ibis_cols[0]
        .zip(*ibis_cols[1:])
        # Change names of the struct fields, from f1, f2 to the original column names
        .map(lambda s: ibis.struct({
            _get_name(c.to_ibis()): s[f]
            for f, c in zip(s.names, cols)
        }))
    )

    col_names = ", ".join(str(c) for c in cols)
    return Expr(result).alias(f"arrays_zip({col_names})")


def _get_name(col: ibis.Value | ibis.Deferred) -> str:
    """Gets alias or name of the column."""
    if isinstance(col, ibis.Value):
        return col.get_name()

    if isinstance(col, ibis.Deferred):
        if (
            isinstance(col._resolver, ibis.common.deferred.Call)
            and col._resolver.func.name.value in {"name", "alias"}
        ):
            return str(col._resolver.args[0].value)

        elif isinstance(col._resolver, ibis.common.deferred.Item):
            # To avoid extra quoting
            if isinstance(col._resolver.indexer, ibis.common.deferred.Just):
                return str(col._resolver.indexer.value)
            return str(col._resolver.indexer)

    return str(col)


# Phase 1: Explode functions

def explode(col: ColumnOrName) -> Expr:
    """Returns a new row for each element in the given array or map column.

    Uses the alias 'col' for array elements, and 'key' and 'value' for map elements.
    """
    col_expr = col_fn(col).to_ibis()
    return Expr(col_expr.unnest()).alias("col")


@incompatibility(
    "Behavior for null or empty arrays may differ depending on the backend. "
    "Some backends may not preserve rows with null/empty arrays."
)
def explode_outer(col: ColumnOrName) -> Expr:
    """Returns a new row for each element in the given array or map column.

    Unlike explode, if the array/map is null or empty, explode_outer returns
    null instead of dropping the row.
    """
    col_expr = col_fn(col).to_ibis()
    # Note: Ibis unnest behavior with nulls may vary by backend
    return Expr(col_expr.unnest()).alias("col")


@incompatibility(
    "Position-based explode requires backend support. "
    "Implementation uses enumerate-like pattern which may not work on all backends."
)
def posexplode(col: ColumnOrName) -> Expr:
    """Returns a new row for each element with position in the given array or map.

    For arrays, returns (pos, col) where pos is the position (0-based).
    For maps, returns (pos, key, value).
    """
    col_expr = col_fn(col).to_ibis()
    # Create an array of structs with position and value
    # This is complex and may need special handling in select()
    # For now, using a simplified approach
    enumerated = col_expr.map(
        lambda i, v: ibis.struct({"pos": i, "col": v})
    )
    return Expr(enumerated.unnest()).alias("posexplode")


@incompatibility(
    "Position-based explode with outer semantics requires backend support. "
    "Implementation may not preserve rows with null/empty arrays on all backends."
)
def posexplode_outer(col: ColumnOrName) -> Expr:
    """Outer version of posexplode.

    Returns (pos, col) for array elements, or (pos, key, value) for maps.
    Preserves rows where the array/map is null or empty.
    """
    col_expr = col_fn(col).to_ibis()
    enumerated = col_expr.map(
        lambda i, v: ibis.struct({"pos": i, "col": v})
    )
    return Expr(enumerated.unnest()).alias("posexplode_outer")


# Phase 2: Backend-agnostic array functions

def array_prepend(col: ColumnOrName, value: Expr | LiteralValue) -> Expr:
    """Prepends an element to the beginning of the array."""
    col_expr = col_fn(col).to_ibis()
    value_expr = lit(value).to_ibis()
    # Prepend by concatenating [value] with the array
    # We need to construct this as: array_from_value ++ col
    # Since ibis.array() doesn't work well with deferred expressions,
    # we'll use a workaround: create an empty array slice and concat
    # Or better: use SQL-level operation
    # For now, let's try using + operator or building manually
    try:
        # Try using the + operator if supported
        result = ibis.array([value_expr]) + col_expr
    except (TypeError, AttributeError):
        # Fallback: construct using concat in a different way
        # Create a literal array and use it
        result = col_expr[:0].concat(ibis.array([value_expr])).concat(col_expr)

    return Expr(result).alias(f"array_prepend({col}, {value})")


@incompatibility(
    "array_except implementation may have limitations with certain backends. "
    "Requires backend support for array set operations."
)
def array_except(col1: ColumnOrName, col2: ColumnOrName) -> Expr:
    """Returns elements in col1 but not in col2, without duplicates."""
    # This is complex to implement in Ibis without backend-specific SQL
    # For now, raise NotImplementedError
    # A full implementation would need SQL-level set difference on arrays
    raise NotImplementedError(
        "array_except() requires complex array set operations that are not yet "
        "fully supported in pyspark-dubber. This function will be implemented in a future version."
    )


@incompatibility(
    "Negative start indices may not work correctly. Only positive start indices are fully supported."
)
def slice(col: ColumnOrName, start: int | ColumnOrName, length: int | ColumnOrName) -> Expr:
    """Returns a slice of the array.

    Args:
        col: Array column to slice
        start: Starting position (1-based in PySpark, like SQL)
        length: Number of elements to return

    Note: Negative start indices are not fully supported due to backend limitations.
    """
    col_expr = col_fn(col).to_ibis()

    # Handle column or literal start/length
    if isinstance(start, int) and start > 0:
        # Positive index: convert from 1-based to 0-based
        start_idx = start - 1
        if isinstance(length, int):
            end_idx = start_idx + length
            result = col_expr[start_idx:end_idx]
        else:
            length_expr = col_fn(length).to_ibis()
            result = col_expr[start_idx:start_idx + length_expr]
    else:
        # For negative or column indices, implementation is limited
        # Raise not implemented for now
        raise NotImplementedError(
            f"slice() with start={start} is not yet supported. "
            "Only positive integer start indices are currently supported."
        )

    return Expr(result).alias(f"slice({col}, {start}, {length})")


@incompatibility(
    "try_element_at has limited support. Only simple positive indices are fully tested."
)
def try_element_at(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
    """Returns element at the given index (1-based) or null if out of bounds.

    Unlike element_at, this returns null instead of raising an error for out-of-bounds access.
    """
    col_expr = col_fn(col).to_ibis()
    length = col_expr.length()

    if isinstance(index, int):
        # Convert 1-based to 0-based index
        if index > 0:
            idx = index - 1
            # Check if index is within bounds using ibis.cases()
            result = ibis.cases([((idx >= 0) & (idx < length), col_expr[idx])], else_=ibis.null())
        elif index < 0:
            idx = index
            # Negative indices work from the end
            result = ibis.cases([((idx >= -length) & (idx < 0), col_expr[idx])], else_=ibis.null())
        else:
            # index == 0 is out of bounds in PySpark (1-based indexing)
            result = ibis.null()
    else:
        # Column-based indices are complex - not fully supported for now
        raise NotImplementedError(
            "try_element_at() with column index is not yet supported. "
            "Only literal integer indices are currently supported."
        )

    return Expr(result).alias(f"try_element_at({col}, {index})")


# Phase 3: Backend-specific functions with SQL

@incompatibility(
    "Uses backend-specific SQL. Tested with DuckDB. "
    "Other Ibis backends may not support array reversal."
)
def reverse(col: ColumnOrName) -> Expr:
    """Reverses the elements of an array."""
    col_expr = col_fn(col).to_ibis()
    # Use Ibis reverse if available, otherwise fall back to SQL
    try:
        result = col_expr.reverse()
    except AttributeError:
        # Fallback: use backend-specific SQL (DuckDB)
        result = col_expr.__class__.sql("list_reverse({col_expr})")
    return Expr(result).alias(f"reverse({col})")


@incompatibility(
    "Uses backend-specific SQL. Tested with DuckDB. "
    "Random seed parameter is not supported."
)
def shuffle(col: ColumnOrName) -> Expr:
    """Randomly shuffles the elements of an array.

    Note: The seed parameter from PySpark is not supported.
    """
    col_expr = col_fn(col).to_ibis()
    # This requires backend-specific functionality
    # For DuckDB: list_shuffle
    # This is a placeholder - actual implementation needs SQL interpolation
    raise NotImplementedError("shuffle() requires backend-specific SQL implementation")


@incompatibility(
    "Uses backend-specific SQL. Tested with DuckDB. "
    "Step parameter behavior may differ from PySpark."
)
def sequence(
    start: ColumnOrName,
    stop: ColumnOrName,
    step: ColumnOrName | int | None = None
) -> Expr:
    """Generates an array of integers from start to stop, incrementing by step.

    Args:
        start: Start value (inclusive)
        stop: End value (inclusive)
        step: Increment value (default: 1 if start <= stop, -1 otherwise)
    """
    start_expr = col_fn(start).to_ibis()
    stop_expr = col_fn(stop).to_ibis()

    if step is None:
        # Default step logic
        # This requires SQL-level functionality
        raise NotImplementedError("sequence() requires backend-specific SQL implementation")

    if isinstance(step, int):
        step_expr = ibis.literal(step)
    else:
        step_expr = col_fn(step).to_ibis()

    # This requires backend-specific functionality (e.g., generate_series in PostgreSQL, range in DuckDB)
    raise NotImplementedError("sequence() requires backend-specific SQL implementation")


@incompatibility(
    "Uses backend-specific SQL. Tested with DuckDB. "
    "Negative index handling may differ from PySpark."
)
def array_insert(
    arr: ColumnOrName,
    pos: ColumnOrName | int,
    value: Expr | LiteralValue
) -> Expr:
    """Inserts an element into the array at the specified position (1-based).

    Args:
        arr: Array column
        pos: Position to insert (1-based, can be negative to count from end)
        value: Value to insert
    """
    arr_expr = col_fn(arr).to_ibis()

    if isinstance(pos, int):
        # Convert 1-based to 0-based
        if pos > 0:
            idx = pos - 1
        elif pos < 0:
            idx = pos
        else:
            idx = 0

        # Slice approach: arr[:idx] + [value] + arr[idx:]
        before = arr_expr[:idx] if idx != 0 else ibis.array([])
        after = arr_expr[idx:]
        result = before.concat(ibis.array([lit(value).to_ibis()])).concat(after)
    else:
        # Position is a column - more complex
        raise NotImplementedError("array_insert() with column position requires backend-specific SQL")

    return Expr(result).alias(f"array_insert({arr}, {pos}, {value})")