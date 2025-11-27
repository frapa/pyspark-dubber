class PySparkException(Exception):
    pass


class AnalysisException(PySparkException):
    pass


class SessionNotSameException(PySparkException):
    pass


class TempTableAlreadyExistsException(AnalysisException):
    pass


class ParseException(AnalysisException):
    pass


class IllegalArgumentException(PySparkException):
    pass


class ArithmeticException(PySparkException):
    pass


class UnsupportedOperationException(PySparkException):
    pass


class ArrayIndexOutOfBoundsException(PySparkException):
    pass


class DateTimeException(PySparkException):
    pass


class NumberFormatException(IllegalArgumentException):
    pass


class StreamingQueryException(PySparkException):
    pass


class QueryExecutionException(PySparkException):
    pass


class PythonException(PySparkException):
    pass


class SparkRuntimeException(PySparkException):
    pass


class SparkUpgradeException(PySparkException):
    pass


class UnknownException(PySparkException):
    pass


class PySparkValueError(PySparkException, ValueError):
    pass


class PySparkTypeError(PySparkException, TypeError):
    pass


class PySparkAttributeError(PySparkException, AttributeError):
    pass


class PySparkRuntimeError(PySparkException, RuntimeError):
    pass


class PySparkAssertionError(PySparkException, AssertionError):
    pass


class PySparkNotImplementedError(PySparkException, NotImplementedError):
    pass
