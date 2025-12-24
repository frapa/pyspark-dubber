from pyspark_dubber.sql.types import DataType, StructType, StructField, StringType, TimestampType


def test_fromDDL() -> None:
    assert DataType.fromDDL("""
        filename STRING,
        type STRING,
        time TIMESTAMP,
        uid STRING,
        sid STRING,
        ip STRING,
        trace_id STRING,
        name STRING,
        result STRING,
        request_params STRING,
        result_params STRING
    """) == StructType([
        StructField("filename", StringType()),
        StructField("type", StringType()),
        StructField("time", TimestampType()),
        StructField("uid", StringType()),
        StructField("sid", StringType()),
        StructField("ip", StringType()),
        StructField("trace_id", StringType()),
        StructField("name", StringType()),
        StructField("result", StringType()),
        StructField("request_params", StringType()),
        StructField("result_params", StringType()),
    ])
