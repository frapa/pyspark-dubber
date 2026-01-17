import inspect
from collections.abc import Callable

from pyspark_dubber.docs import incompatibility
from pyspark_dubber.sql.expr import Expr, LiteralValue
from pyspark_dubber.sql.functions._helper import sql_func
from pyspark_dubber.sql.functions.normal import ColumnOrName, col as col_fn
from pyspark_dubber.sql.functions.array import array_size

UnaryOrBinary = Callable[[Expr], Expr] | Callable[[Expr, Expr], Expr]


def size(col: ColumnOrName) -> Expr:
    return array_size(col).alias(f"size({col})")


def element_at(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
    col_expr = col_fn(col).to_ibis()
    if isinstance(index, int):
        # Convert 1-based to 0-based index
        # Spark: positive indices are 1-based, negative indices work from end (-1 is last)
        if index > 0:
            idx = index - 1
        else:
            idx = index
    else:
        idx = col_fn(index).to_ibis() - 1
    return Expr(col_expr[idx]).alias(f"element_at({col}, {index})")


def get(col: ColumnOrName, index: ColumnOrName | int) -> Expr:
    return element_at(col, index).alias(f"get({col}, {index})")


@incompatibility("comparator parameter is not supported")
def array_sort(col: ColumnOrName, comparator=None) -> Expr:
    col_expr = col_fn(col).to_ibis()
    result = col_expr.sort()
    # PySpark displays the internal lambda function as the column name
    # We replicate this for compatibility, though it's not user-friendly
    ugly_name = f"array_sort({col}, lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable()))"
    return Expr(result).alias(ugly_name)


@sql_func(col_name_args="col")
def filter(col: ColumnOrName, f: UnaryOrBinary) -> Expr:
    if len(inspect.signature(f).parameters) == 1:
        ibis_func = lambda v: f(Expr(v)).to_ibis()
    else:
        ibis_func = lambda v, i: f(Expr(v), Expr(i)).to_ibis()

    return col.filter(ibis_func)


@sql_func(col_name_args="col")
def transform(col: ColumnOrName, f: UnaryOrBinary) -> Expr:
    if len(inspect.signature(f).parameters) == 1:
        ibis_func = lambda v: f(Expr(v)).to_ibis()
    else:
        ibis_func = lambda v, i: f(Expr(v), Expr(i)).to_ibis()

    return col.map(ibis_func)


# Additional collection functions

def cardinality(col: ColumnOrName) -> Expr:
    """Returns the size of the collection (array or map).

    Returns -1 for null arrays/maps (PySpark behavior).
    """
    import ibis
    col_expr = col_fn(col).to_ibis()
    # PySpark returns -1 for null, not null
    # Use ibis.cases() for conditional logic
    result = ibis.cases([(col_expr.isnull(), -1)], else_=col_expr.length())
    return Expr(result).alias(f"cardinality({col})")


@sql_func(col_name_args="col")
def exists(col: ColumnOrName, f: Callable[[Expr], Expr]) -> Expr:
    """Returns true if any element in the array satisfies the predicate.

    Args:
        col: Array column
        f: Predicate function that takes an element and returns a boolean
    """
    ibis_func = lambda v: f(Expr(v)).to_ibis()
    # Check if any element satisfies the predicate
    return col.filter(ibis_func).length() > 0


@sql_func(col_name_args="col")
def forall(col: ColumnOrName, f: Callable[[Expr], Expr]) -> Expr:
    """Returns true if all elements in the array satisfy the predicate.

    Args:
        col: Array column
        f: Predicate function that takes an element and returns a boolean
    """
    ibis_func = lambda v: f(Expr(v)).to_ibis()
    # All satisfy if filtered array has same length as original
    return col.filter(ibis_func).length() == col.length()


def zip_with(
    left: ColumnOrName,
    right: ColumnOrName,
    f: Callable[[Expr, Expr], Expr]
) -> Expr:
    """Merges two arrays element-wise using the given function.

    Args:
        left: First array column
        right: Second array column
        f: Function that takes two elements (one from each array) and returns a value
    """
    left_expr = col_fn(left).to_ibis()
    right_expr = col_fn(right).to_ibis()

    # Zip the two arrays together
    zipped = left_expr.zip(right_expr)

    # Map over the zipped array, applying f to each pair
    ibis_func = lambda s: f(Expr(s[0]), Expr(s[1])).to_ibis()
    result = zipped.map(ibis_func)

    return Expr(result).alias(f"zip_with({left}, {right}, lambdafunction(...))")


@incompatibility(
    "Reduce/aggregate with custom merge and finish functions requires backend support. "
    "Implementation may be limited or unsupported on some backends."
)
def aggregate(
    col: ColumnOrName,
    initialValue: Expr | LiteralValue,
    merge: Callable[[Expr, Expr], Expr],
    finish: Callable[[Expr], Expr] | None = None
) -> Expr:
    """Aggregates elements of an array using a merge function and optional finish function.

    Args:
        col: Array column to aggregate
        initialValue: Initial accumulator value
        merge: Function that merges accumulator with current element
        finish: Optional function to transform the final accumulator

    Note: This function has limited backend support and may not work on all Ibis backends.
    """
    # This is a complex operation that may require backend-specific SQL
    # For now, we'll raise NotImplementedError
    # A full implementation would need to use recursive CTEs or backend-specific reduce functions
    raise NotImplementedError(
        "aggregate() requires backend-specific SQL implementation. "
        "This function is not yet supported in pyspark-dubber."
    )


# Alias for aggregate
def reduce(
    col: ColumnOrName,
    initialValue: Expr | LiteralValue,
    merge: Callable[[Expr, Expr], Expr],
    finish: Callable[[Expr], Expr] | None = None
) -> Expr:
    """Alias for aggregate()."""
    return aggregate(col, initialValue, merge, finish)
