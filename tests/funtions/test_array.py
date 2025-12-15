from tests.conftest import comparison_test


@comparison_test
def test_array_contains(spark, load) -> None:
  functions = load("sql.functions")

  df = spark.createDataFrame(
      [(1, [1,2]), (2, [3,4])],
      ("a", "b"),
  )

  df.select(
      "*",
      functions.array_contains("b", 1),
      functions.array_contains(df.b, df.a),
  ).show()

@comparison_test
def test_array_append(spark, load) -> None:
  functions = load("sql.functions")

  df = spark.createDataFrame(
      [(1, [1,2]), (None, [3,4])],
      ("a", "b"),
  )
  df.select(
      "*",
      functions.array_append("a", "1"),
      functions.array_append(df.b, df.a),
  ).show()