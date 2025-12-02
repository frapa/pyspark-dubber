# API Coverage

This page shows which APIs are currently re-implemented by `pyspark-dubber`. This list is not exhaustive, showing mostly public functions and DataFrame APIs, however some additional APIs and magic methods are also implemented.

In addition to that, certain pyspark APIs are partially implemented, for example not all parameters or parameter types are supported. In spite of that, they are listed as implemented in the tables below, with notes in case of partial implementation.

The overall approximate API coverage (with the caveats above) is 19.7%. We prioritize implementing commonly used functions, as pyspark has many esoteric APIs.

## SparkSession (3/19 = 16%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`SparkSession.Builder`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.Builder.html) | :material-check: |  |
| [`SparkSession.active`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.active.html) |   |  |
| [`SparkSession.addArtifact`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.addArtifact.html) |   |  |
| [`SparkSession.addArtifacts`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.addArtifacts.html) |   |  |
| [`SparkSession.addTag`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.addTag.html) |   |  |
| [`SparkSession.clearTags`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.clearTags.html) |   |  |
| [`SparkSession.copyFromLocalToFs`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.copyFromLocalToFs.html) |   |  |
| [`SparkSession.createDataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html) | :material-check: |  |
| [`SparkSession.getActiveSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.getActiveSession.html) |   |  |
| [`SparkSession.getTags`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.getTags.html) |   |  |
| [`SparkSession.interruptAll`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.interruptAll.html) |   |  |
| [`SparkSession.interruptOperation`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.interruptOperation.html) |   |  |
| [`SparkSession.interruptTag`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.interruptTag.html) |   |  |
| [`SparkSession.newSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.newSession.html) |   |  |
| [`SparkSession.range`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.range.html) |   |  |
| [`SparkSession.removeTag`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.removeTag.html) |   |  |
| [`SparkSession.sql`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html) |   |  |
| [`SparkSession.stop`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.stop.html) | :material-check: |  |
| [`SparkSession.table`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.table.html) |   |  |

## SparkSession.builder (3/7 = 43%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`Builder.appName`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.appName.html) | :material-check: |  |
| [`Builder.config`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.config.html) |   |  |
| [`Builder.create`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.create.html) |   |  |
| [`Builder.enableHiveSupport`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.enableHiveSupport.html) |   |  |
| [`Builder.getOrCreate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.getOrCreate.html) | :material-check: |  |
| [`Builder.master`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.master.html) | :material-check: |  |
| [`Builder.remote`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.remote.html) |   |  |

## Input Formats (2/12 = 17%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`DataFrameReader.csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html) |   |  |
| [`DataFrameReader.format`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.format.html) |   |  |
| [`DataFrameReader.jdbc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.jdbc.html) |   |  |
| [`DataFrameReader.json`](/pyspark-dubber/API Reference/DataFrameReader/DataFrameReader.json) | :material-check: | Most parameters are accepted but completely ignored. |
| [`DataFrameReader.load`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html) |   |  |
| [`DataFrameReader.option`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.option.html) |   |  |
| [`DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html) |   |  |
| [`DataFrameReader.orc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.orc.html) |   |  |
| [`DataFrameReader.parquet`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.parquet.html) | :material-check: |  |
| [`DataFrameReader.schema`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.schema.html) |   |  |
| [`DataFrameReader.table`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.table.html) |   |  |
| [`DataFrameReader.text`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.text.html) |   |  |

## Output Formats (3/16 = 19%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`DataFrameWriter.bucketBy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.bucketBy.html) |   |  |
| [`DataFrameWriter.csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.csv.html) | :material-check: |  |
| [`DataFrameWriter.format`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.format.html) |   |  |
| [`DataFrameWriter.insertInto`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.insertInto.html) |   |  |
| [`DataFrameWriter.jdbc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.jdbc.html) |   |  |
| [`DataFrameWriter.json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.json.html) |   |  |
| [`DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html) | :material-check: |  |
| [`DataFrameWriter.option`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.option.html) | :material-check: |  |
| [`DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html) |   |  |
| [`DataFrameWriter.orc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.orc.html) |   |  |
| [`DataFrameWriter.parquet`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html) |   |  |
| [`DataFrameWriter.partitionBy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html) |   |  |
| [`DataFrameWriter.save`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.save.html) |   |  |
| [`DataFrameWriter.saveAsTable`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html) |   |  |
| [`DataFrameWriter.sortBy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.sortBy.html) |   |  |
| [`DataFrameWriter.text`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.text.html) |   |  |

## DataFrame (34/94 = 36%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`DataFrame.agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html) | :material-check: |  |
| [`DataFrame.alias`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.alias.html) | :material-check: |  |
| [`DataFrame.approxQuantile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html) |   |  |
| [`DataFrame.cache`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html) | :material-check: |  |
| [`DataFrame.checkpoint`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html) |   |  |
| [`DataFrame.coalesce`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html) |   |  |
| [`DataFrame.colRegex`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.colRegex.html) |   |  |
| [`DataFrame.collect`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.collect.html) | :material-check: |  |
| [`DataFrame.corr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.corr.html) |   |  |
| [`DataFrame.count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html) | :material-check: |  |
| [`DataFrame.cov`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cov.html) |   |  |
| [`DataFrame.createGlobalTempView`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createGlobalTempView.html) |   |  |
| [`DataFrame.createOrReplaceGlobalTempView`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceGlobalTempView.html) |   |  |
| [`DataFrame.createOrReplaceTempView`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html) |   |  |
| [`DataFrame.createTempView`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createTempView.html) |   |  |
| [`DataFrame.crossJoin`](/pyspark-dubber/API Reference/DataFrame/DataFrame.crossJoin) | :material-check: | pyspark allows duplicate column names, and by default does not prefix/suffix the columns of the other dataframe at all. Our backend (ibis) currently does not support duplicate column names, so this function suffixes all columns on other with '_right'. |
| [`DataFrame.crosstab`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crosstab.html) |   |  |
| [`DataFrame.cube`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cube.html) |   |  |
| [`DataFrame.describe`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.describe.html) |   |  |
| [`DataFrame.distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html) | :material-check: |  |
| [`DataFrame.drop`](/pyspark-dubber/API Reference/DataFrame/DataFrame.drop) | :material-check: | Our backend (ibis) does not support duplicate column names like pyspark, therefore this function does not support dropping columns with the same name. You cannot anyway currently create such dataframes using `pyspark-dubber`. |
| [`DataFrame.dropDuplicates`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html) | :material-check: |  |
| [`DataFrame.dropDuplicatesWithinWatermark`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html) |   |  |
| [`DataFrame.drop_duplicates`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop_duplicates.html) | :material-check: |  |
| [`DataFrame.dropna`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropna.html) |   |  |
| [`DataFrame.exceptAll`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html) |   |  |
| [`DataFrame.explain`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html) |   |  |
| [`DataFrame.fillna`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.fillna.html) | :material-check: |  |
| [`DataFrame.filter`](/pyspark-dubber/API Reference/DataFrame/DataFrame.filter) | :material-check: | Using a string as a SQL expressions is not supported yet. |
| [`DataFrame.first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.first.html) | :material-check: |  |
| [`DataFrame.foreach`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.foreach.html) |   |  |
| [`DataFrame.foreachPartition`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.foreachPartition.html) |   |  |
| [`DataFrame.freqItems`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.freqItems.html) |   |  |
| [`DataFrame.groupBy`](/pyspark-dubber/API Reference/DataFrame/DataFrame.groupBy) | :material-check: | Currently only column names are supported for grouping, column expressions are not supported. |
| [`DataFrame.groupby`](/pyspark-dubber/API Reference/DataFrame/DataFrame.groupby) | :material-check: | Currently only column names are supported for grouping, column expressions are not supported. |
| [`DataFrame.head`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.head.html) | :material-check: |  |
| [`DataFrame.hint`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.hint.html) |   |  |
| [`DataFrame.inputFiles`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.inputFiles.html) |   |  |
| [`DataFrame.intersect`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersect.html) |   |  |
| [`DataFrame.intersectAll`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersectAll.html) |   |  |
| [`DataFrame.isEmpty`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.isEmpty.html) | :material-check: |  |
| [`DataFrame.isLocal`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.isLocal.html) |   |  |
| [`DataFrame.join`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html) | :material-check: |  |
| [`DataFrame.limit`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html) | :material-check: |  |
| [`DataFrame.localCheckpoint`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.localCheckpoint.html) |   |  |
| [`DataFrame.mapInArrow`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInArrow.html) |   |  |
| [`DataFrame.mapInPandas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html) |   |  |
| [`DataFrame.melt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.melt.html) |   |  |
| [`DataFrame.observe`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.observe.html) |   |  |
| [`DataFrame.offset`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.offset.html) | :material-check: |  |
| [`DataFrame.orderBy`](/pyspark-dubber/API Reference/DataFrame/DataFrame.orderBy) | :material-check: | Sorting by column ordinals (which are 1-based, not 0-based) is not supported yet. Additionally, this function still needs better testing around edge cases, when sorting with complex column expressions which include sorting. |
| [`DataFrame.pandas_api`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.pandas_api.html) |   |  |
| [`DataFrame.persist`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html) |   |  |
| [`DataFrame.printSchema`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html) | :material-check: |  |
| [`DataFrame.randomSplit`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html) |   |  |
| [`DataFrame.registerTempTable`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.registerTempTable.html) |   |  |
| [`DataFrame.repartition`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html) |   |  |
| [`DataFrame.repartitionByRange`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartitionByRange.html) |   |  |
| [`DataFrame.replace`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.replace.html) |   |  |
| [`DataFrame.rollup`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.rollup.html) |   |  |
| [`DataFrame.sameSemantics`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sameSemantics.html) |   |  |
| [`DataFrame.sample`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html) |   |  |
| [`DataFrame.sampleBy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sampleBy.html) |   |  |
| [`DataFrame.select`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html) | :material-check: |  |
| [`DataFrame.selectExpr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.selectExpr.html) |   |  |
| [`DataFrame.semanticHash`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.semanticHash.html) |   |  |
| [`DataFrame.show`](/pyspark-dubber/API Reference/DataFrame/DataFrame.show) | :material-check: | The `truncate` and `vertical` parameters are not honored. Additionally, the output is not printed justified exactly as pyspark as of the current version. |
| [`DataFrame.sort`](/pyspark-dubber/API Reference/DataFrame/DataFrame.sort) | :material-check: | Sorting by column ordinals (which are 1-based, not 0-based) is not supported yet. Additionally, this function still needs better testing around edge cases, when sorting with complex column expressions which include sorting. |
| [`DataFrame.sortWithinPartitions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sortWithinPartitions.html) |   |  |
| [`DataFrame.subtract`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.subtract.html) |   |  |
| [`DataFrame.summary`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.summary.html) |   |  |
| [`DataFrame.tail`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.tail.html) | :material-check: |  |
| [`DataFrame.take`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.take.html) | :material-check: |  |
| [`DataFrame.to`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.to.html) |   |  |
| [`DataFrame.toDF`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toDF.html) |   |  |
| [`DataFrame.toJSON`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toJSON.html) |   |  |
| [`DataFrame.toLocalIterator`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html) |   |  |
| [`DataFrame.toPandas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html) | :material-check: |  |
| [`DataFrame.to_koalas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.to_koalas.html) |   |  |
| [`DataFrame.to_pandas_on_spark`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.to_pandas_on_spark.html) |   |  |
| [`DataFrame.transform`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html) |   |  |
| [`DataFrame.union`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html) |   |  |
| [`DataFrame.unionAll`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionAll.html) |   |  |
| [`DataFrame.unionByName`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html) | :material-check: |  |
| [`DataFrame.unpersist`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpersist.html) |   |  |
| [`DataFrame.unpivot`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html) |   |  |
| [`DataFrame.where`](/pyspark-dubber/API Reference/DataFrame/DataFrame.where) | :material-check: | Using a string as a SQL expressions is not supported yet. |
| [`DataFrame.withColumn`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html) | :material-check: |  |
| [`DataFrame.withColumnRenamed`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html) | :material-check: |  |
| [`DataFrame.withColumns`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html) | :material-check: |  |
| [`DataFrame.withColumnsRenamed`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html) | :material-check: |  |
| [`DataFrame.withMetadata`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withMetadata.html) |   |  |
| [`DataFrame.withWatermark`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html) |   |  |
| [`DataFrame.writeTo`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.writeTo.html) |   |  |

## GroupBy (2/12 = 17%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`GroupedData.agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html) | :material-check: |  |
| [`GroupedData.apply`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.apply.html) |   |  |
| [`GroupedData.applyInPandas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html) |   |  |
| [`GroupedData.applyInPandasWithState`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandasWithState.html) |   |  |
| [`GroupedData.avg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.avg.html) |   |  |
| [`GroupedData.cogroup`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.cogroup.html) |   |  |
| [`GroupedData.count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.count.html) | :material-check: |  |
| [`GroupedData.max`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.max.html) |   |  |
| [`GroupedData.mean`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.mean.html) |   |  |
| [`GroupedData.min`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.min.html) |   |  |
| [`GroupedData.pivot`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html) |   |  |
| [`GroupedData.sum`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.sum.html) |   |  |

## Column (24/32 = 75%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`Column.alias`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html) | :material-check: |  |
| [`Column.asc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc.html) | :material-check: |  |
| [`Column.asc_nulls_first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc_nulls_first.html) | :material-check: |  |
| [`Column.asc_nulls_last`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc_nulls_last.html) | :material-check: |  |
| [`Column.astype`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.astype.html) | :material-check: |  |
| [`Column.between`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.between.html) | :material-check: |  |
| [`Column.bitwiseAND`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.bitwiseAND.html) | :material-check: |  |
| [`Column.bitwiseOR`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.bitwiseOR.html) | :material-check: |  |
| [`Column.bitwiseXOR`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.bitwiseXOR.html) | :material-check: |  |
| [`Column.cast`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.cast.html) | :material-check: |  |
| [`Column.contains`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.contains.html) | :material-check: |  |
| [`Column.desc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc.html) | :material-check: |  |
| [`Column.desc_nulls_first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc_nulls_first.html) | :material-check: |  |
| [`Column.desc_nulls_last`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc_nulls_last.html) | :material-check: |  |
| [`Column.dropFields`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.dropFields.html) |   |  |
| [`Column.endswith`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.endswith.html) | :material-check: |  |
| [`Column.eqNullSafe`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.eqNullSafe.html) |   |  |
| [`Column.getField`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.getField.html) |   |  |
| [`Column.getItem`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.getItem.html) |   |  |
| [`Column.ilike`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.ilike.html) | :material-check: |  |
| [`Column.isNotNull`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNotNull.html) | :material-check: |  |
| [`Column.isNull`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNull.html) | :material-check: |  |
| [`Column.isin`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isin.html) |   |  |
| [`Column.like`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.like.html) | :material-check: |  |
| [`Column.name`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.name.html) | :material-check: |  |
| [`Column.otherwise`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.otherwise.html) |   |  |
| [`Column.over`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.over.html) |   |  |
| [`Column.rlike`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.rlike.html) | :material-check: |  |
| [`Column.startswith`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.startswith.html) | :material-check: |  |
| [`Column.substr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.substr.html) | :material-check: |  |
| [`Column.when`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.when.html) |   |  |
| [`Column.withField`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.withField.html) |   |  |

## Functions (50/422 = 12%)

| API | Implemented | Notes |
| --- | :---------: | ----- |
| [`pyspark.sql.functions.abs`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.abs.html) | :material-check: |  |
| [`pyspark.sql.functions.acos`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.acos.html) | :material-check: |  |
| [`pyspark.sql.functions.acosh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.acosh.html) |   |  |
| [`pyspark.sql.functions.add_months`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.add_months.html) |   |  |
| [`pyspark.sql.functions.aes_decrypt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aes_decrypt.html) |   |  |
| [`pyspark.sql.functions.aes_encrypt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aes_encrypt.html) |   |  |
| [`pyspark.sql.functions.aggregate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aggregate.html) |   |  |
| [`pyspark.sql.functions.any_value`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.any_value.html) |   |  |
| [`pyspark.sql.functions.approxCountDistinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approxCountDistinct.html) |   |  |
| [`pyspark.sql.functions.approx_count_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html) |   |  |
| [`pyspark.sql.functions.approx_percentile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_percentile.html) |   |  |
| [`pyspark.sql.functions.array`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html) |   |  |
| [`pyspark.sql.functions.array_agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_agg.html) |   |  |
| [`pyspark.sql.functions.array_append`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_append.html) |   |  |
| [`pyspark.sql.functions.array_compact`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_compact.html) |   |  |
| [`pyspark.sql.functions.array_contains`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_contains.html) |   |  |
| [`pyspark.sql.functions.array_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_distinct.html) |   |  |
| [`pyspark.sql.functions.array_except`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_except.html) |   |  |
| [`pyspark.sql.functions.array_insert`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_insert.html) |   |  |
| [`pyspark.sql.functions.array_intersect`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_intersect.html) |   |  |
| [`pyspark.sql.functions.array_join`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_join.html) |   |  |
| [`pyspark.sql.functions.array_max`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_max.html) |   |  |
| [`pyspark.sql.functions.array_min`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_min.html) |   |  |
| [`pyspark.sql.functions.array_position`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_position.html) |   |  |
| [`pyspark.sql.functions.array_prepend`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_prepend.html) |   |  |
| [`pyspark.sql.functions.array_remove`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_remove.html) |   |  |
| [`pyspark.sql.functions.array_repeat`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_repeat.html) |   |  |
| [`pyspark.sql.functions.array_size`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_size.html) |   |  |
| [`pyspark.sql.functions.array_sort`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_sort.html) |   |  |
| [`pyspark.sql.functions.array_union`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_union.html) |   |  |
| [`pyspark.sql.functions.arrays_overlap`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.arrays_overlap.html) |   |  |
| [`pyspark.sql.functions.arrays_zip`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.arrays_zip.html) |   |  |
| [`pyspark.sql.functions.asc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc.html) | :material-check: |  |
| [`pyspark.sql.functions.asc_nulls_first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc_nulls_first.html) | :material-check: |  |
| [`pyspark.sql.functions.asc_nulls_last`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc_nulls_last.html) | :material-check: |  |
| [`pyspark.sql.functions.ascii`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ascii.html) | :material-check: |  |
| [`pyspark.sql.functions.asin`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asin.html) | :material-check: |  |
| [`pyspark.sql.functions.asinh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asinh.html) |   |  |
| [`pyspark.sql.functions.assert_true`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.assert_true.html) |   |  |
| [`pyspark.sql.functions.atan`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atan.html) | :material-check: |  |
| [`pyspark.sql.functions.atan2`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atan2.html) | :material-check: |  |
| [`pyspark.sql.functions.atanh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atanh.html) |   |  |
| [`pyspark.sql.functions.avg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.avg.html) | :material-check: |  |
| [`pyspark.sql.functions.base64`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.base64.html) |   |  |
| [`pyspark.sql.functions.bin`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bin.html) |   |  |
| [`pyspark.sql.functions.bit_and`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_and.html) |   |  |
| [`pyspark.sql.functions.bit_count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_count.html) |   |  |
| [`pyspark.sql.functions.bit_get`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_get.html) |   |  |
| [`pyspark.sql.functions.bit_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_length.html) |   |  |
| [`pyspark.sql.functions.bit_or`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_or.html) |   |  |
| [`pyspark.sql.functions.bit_xor`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_xor.html) |   |  |
| [`pyspark.sql.functions.bitmap_bit_position`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_bit_position.html) |   |  |
| [`pyspark.sql.functions.bitmap_bucket_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_bucket_number.html) |   |  |
| [`pyspark.sql.functions.bitmap_construct_agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_construct_agg.html) |   |  |
| [`pyspark.sql.functions.bitmap_count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_count.html) |   |  |
| [`pyspark.sql.functions.bitmap_or_agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitmap_or_agg.html) |   |  |
| [`pyspark.sql.functions.bitwiseNOT`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitwiseNOT.html) |   |  |
| [`pyspark.sql.functions.bitwise_not`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitwise_not.html) |   |  |
| [`pyspark.sql.functions.bool_and`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bool_and.html) |   |  |
| [`pyspark.sql.functions.bool_or`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bool_or.html) |   |  |
| [`pyspark.sql.functions.broadcast`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html) |   |  |
| [`pyspark.sql.functions.bround`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bround.html) |   |  |
| [`pyspark.sql.functions.btrim`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.btrim.html) |   |  |
| [`pyspark.sql.functions.bucket`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bucket.html) |   |  |
| [`pyspark.sql.functions.call_function`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html) |   |  |
| [`pyspark.sql.functions.call_udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_udf.html) |   |  |
| [`pyspark.sql.functions.cardinality`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cardinality.html) |   |  |
| [`pyspark.sql.functions.cbrt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cbrt.html) |   |  |
| [`pyspark.sql.functions.ceil`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ceil.html) | :material-check: |  |
| [`pyspark.sql.functions.ceiling`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ceiling.html) |   |  |
| [`pyspark.sql.functions.char`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.char.html) |   |  |
| [`pyspark.sql.functions.char_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.char_length.html) |   |  |
| [`pyspark.sql.functions.character_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.character_length.html) |   |  |
| [`pyspark.sql.functions.coalesce`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html) |   |  |
| [`pyspark.sql.functions.col`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html) | :material-check: |  |
| [`pyspark.sql.functions.collect_list`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_list.html) |   |  |
| [`pyspark.sql.functions.collect_set`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_set.html) |   |  |
| [`pyspark.sql.functions.column`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.column.html) | :material-check: |  |
| [`pyspark.sql.functions.concat`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat.html) |   |  |
| [`pyspark.sql.functions.concat_ws`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat_ws.html) |   |  |
| [`pyspark.sql.functions.contains`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.contains.html) |   |  |
| [`pyspark.sql.functions.conv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.conv.html) |   |  |
| [`pyspark.sql.functions.convert_timezone`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.convert_timezone.html) |   |  |
| [`pyspark.sql.functions.corr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.corr.html) |   |  |
| [`pyspark.sql.functions.cos`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cos.html) | :material-check: |  |
| [`pyspark.sql.functions.cosh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cosh.html) | :material-check: |  |
| [`pyspark.sql.functions.cot`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cot.html) | :material-check: |  |
| [`pyspark.sql.functions.count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html) | :material-check: |  |
| [`pyspark.sql.functions.countDistinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.countDistinct.html) |   |  |
| [`pyspark.sql.functions.count_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_distinct.html) |   |  |
| [`pyspark.sql.functions.count_if`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_if.html) |   |  |
| [`pyspark.sql.functions.count_min_sketch`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_min_sketch.html) |   |  |
| [`pyspark.sql.functions.covar_pop`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.covar_pop.html) |   |  |
| [`pyspark.sql.functions.covar_samp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.covar_samp.html) |   |  |
| [`pyspark.sql.functions.crc32`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.crc32.html) |   |  |
| [`pyspark.sql.functions.create_map`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.create_map.html) |   |  |
| [`pyspark.sql.functions.csc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.csc.html) | :material-check: |  |
| [`pyspark.sql.functions.cume_dist`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cume_dist.html) |   |  |
| [`pyspark.sql.functions.curdate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.curdate.html) |   |  |
| [`pyspark.sql.functions.current_catalog`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_catalog.html) |   |  |
| [`pyspark.sql.functions.current_database`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_database.html) |   |  |
| [`pyspark.sql.functions.current_date`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_date.html) |   |  |
| [`pyspark.sql.functions.current_schema`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_schema.html) |   |  |
| [`pyspark.sql.functions.current_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html) | :material-check: |  |
| [`pyspark.sql.functions.current_timezone`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timezone.html) |   |  |
| [`pyspark.sql.functions.current_user`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_user.html) |   |  |
| [`pyspark.sql.functions.date_add`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_add.html) |   |  |
| [`pyspark.sql.functions.date_diff`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_diff.html) |   |  |
| [`pyspark.sql.functions.date_format`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html) |   |  |
| [`pyspark.sql.functions.date_from_unix_date`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_from_unix_date.html) |   |  |
| [`pyspark.sql.functions.date_part`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_part.html) |   |  |
| [`pyspark.sql.functions.date_sub`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_sub.html) |   |  |
| [`pyspark.sql.functions.date_trunc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_trunc.html) |   |  |
| [`pyspark.sql.functions.dateadd`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dateadd.html) |   |  |
| [`pyspark.sql.functions.datediff`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datediff.html) |   |  |
| [`pyspark.sql.functions.datepart`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datepart.html) |   |  |
| [`pyspark.sql.functions.day`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.day.html) |   |  |
| [`pyspark.sql.functions.dayofmonth`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofmonth.html) |   |  |
| [`pyspark.sql.functions.dayofweek`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html) |   |  |
| [`pyspark.sql.functions.dayofyear`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofyear.html) |   |  |
| [`pyspark.sql.functions.days`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.days.html) |   |  |
| [`pyspark.sql.functions.decode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.decode.html) |   |  |
| [`pyspark.sql.functions.degrees`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.degrees.html) | :material-check: |  |
| [`pyspark.sql.functions.dense_rank`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html) |   |  |
| [`pyspark.sql.functions.desc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc.html) | :material-check: |  |
| [`pyspark.sql.functions.desc_nulls_first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc_nulls_first.html) | :material-check: |  |
| [`pyspark.sql.functions.desc_nulls_last`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc_nulls_last.html) | :material-check: |  |
| [`pyspark.sql.functions.e`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.e.html) | :material-check: |  |
| [`pyspark.sql.functions.element_at`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.element_at.html) |   |  |
| [`pyspark.sql.functions.elt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.elt.html) |   |  |
| [`pyspark.sql.functions.encode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.encode.html) |   |  |
| [`pyspark.sql.functions.endswith`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.endswith.html) |   |  |
| [`pyspark.sql.functions.equal_null`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.equal_null.html) |   |  |
| [`pyspark.sql.functions.every`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.every.html) |   |  |
| [`pyspark.sql.functions.exists`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exists.html) |   |  |
| [`pyspark.sql.functions.exp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exp.html) | :material-check: |  |
| [`pyspark.sql.functions.explode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) |   |  |
| [`pyspark.sql.functions.explode_outer`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode_outer.html) |   |  |
| [`pyspark.sql.functions.expm1`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expm1.html) |   |  |
| [`pyspark.sql.functions.expr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html) |   |  |
| [`pyspark.sql.functions.extract`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.extract.html) |   |  |
| [`pyspark.sql.functions.factorial`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.factorial.html) |   |  |
| [`pyspark.sql.functions.filter`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.filter.html) |   |  |
| [`pyspark.sql.functions.find_in_set`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.find_in_set.html) |   |  |
| [`pyspark.sql.functions.first`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.first.html) |   |  |
| [`pyspark.sql.functions.first_value`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.first_value.html) |   |  |
| [`pyspark.sql.functions.flatten`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.flatten.html) |   |  |
| [`pyspark.sql.functions.floor`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.floor.html) | :material-check: |  |
| [`pyspark.sql.functions.forall`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.forall.html) |   |  |
| [`pyspark.sql.functions.format_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_number.html) |   |  |
| [`pyspark.sql.functions.format_string`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_string.html) |   |  |
| [`pyspark.sql.functions.from_csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_csv.html) |   |  |
| [`pyspark.sql.functions.from_json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html) |   |  |
| [`pyspark.sql.functions.from_unixtime`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_unixtime.html) |   |  |
| [`pyspark.sql.functions.from_utc_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_utc_timestamp.html) |   |  |
| [`pyspark.sql.functions.get`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get.html) |   |  |
| [`pyspark.sql.functions.get_json_object`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get_json_object.html) |   |  |
| [`pyspark.sql.functions.getbit`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.getbit.html) |   |  |
| [`pyspark.sql.functions.greatest`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.greatest.html) |   |  |
| [`pyspark.sql.functions.grouping`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.grouping.html) |   |  |
| [`pyspark.sql.functions.grouping_id`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.grouping_id.html) |   |  |
| [`pyspark.sql.functions.hash`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hash.html) |   |  |
| [`pyspark.sql.functions.hex`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hex.html) |   |  |
| [`pyspark.sql.functions.histogram_numeric`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.histogram_numeric.html) |   |  |
| [`pyspark.sql.functions.hll_sketch_agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hll_sketch_agg.html) |   |  |
| [`pyspark.sql.functions.hll_sketch_estimate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hll_sketch_estimate.html) |   |  |
| [`pyspark.sql.functions.hll_union`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hll_union.html) |   |  |
| [`pyspark.sql.functions.hll_union_agg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hll_union_agg.html) |   |  |
| [`pyspark.sql.functions.hour`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hour.html) |   |  |
| [`pyspark.sql.functions.hours`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hours.html) |   |  |
| [`pyspark.sql.functions.hypot`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hypot.html) |   |  |
| [`pyspark.sql.functions.ifnull`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ifnull.html) |   |  |
| [`pyspark.sql.functions.ilike`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ilike.html) |   |  |
| [`pyspark.sql.functions.initcap`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html) |   |  |
| [`pyspark.sql.functions.inline`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.inline.html) |   |  |
| [`pyspark.sql.functions.inline_outer`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.inline_outer.html) |   |  |
| [`pyspark.sql.functions.input_file_block_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_block_length.html) |   |  |
| [`pyspark.sql.functions.input_file_block_start`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_block_start.html) |   |  |
| [`pyspark.sql.functions.input_file_name`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_name.html) |   |  |
| [`pyspark.sql.functions.instr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.instr.html) |   |  |
| [`pyspark.sql.functions.isnan`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.isnan.html) | :material-check: |  |
| [`pyspark.sql.functions.isnotnull`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.isnotnull.html) | :material-check: |  |
| [`pyspark.sql.functions.isnull`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.isnull.html) | :material-check: |  |
| [`pyspark.sql.functions.java_method`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.java_method.html) |   |  |
| [`pyspark.sql.functions.json_array_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_array_length.html) |   |  |
| [`pyspark.sql.functions.json_object_keys`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_object_keys.html) |   |  |
| [`pyspark.sql.functions.json_tuple`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_tuple.html) |   |  |
| [`pyspark.sql.functions.kurtosis`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.kurtosis.html) |   |  |
| [`pyspark.sql.functions.lag`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html) |   |  |
| [`pyspark.sql.functions.last`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.last.html) |   |  |
| [`pyspark.sql.functions.last_day`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.last_day.html) |   |  |
| [`pyspark.sql.functions.last_value`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.last_value.html) |   |  |
| [`pyspark.sql.functions.lcase`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lcase.html) |   |  |
| [`pyspark.sql.functions.lead`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html) |   |  |
| [`pyspark.sql.functions.least`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.least.html) |   |  |
| [`pyspark.sql.functions.left`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.left.html) |   |  |
| [`pyspark.sql.functions.length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.length.html) |   |  |
| [`pyspark.sql.functions.levenshtein`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.levenshtein.html) |   |  |
| [`pyspark.sql.functions.like`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.like.html) |   |  |
| [`pyspark.sql.functions.lit`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html) | :material-check: |  |
| [`pyspark.sql.functions.ln`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ln.html) | :material-check: |  |
| [`pyspark.sql.functions.localtimestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.localtimestamp.html) |   |  |
| [`pyspark.sql.functions.locate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.locate.html) |   |  |
| [`pyspark.sql.functions.log`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log.html) | :material-check: |  |
| [`pyspark.sql.functions.log10`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log10.html) | :material-check: |  |
| [`pyspark.sql.functions.log1p`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log1p.html) |   |  |
| [`pyspark.sql.functions.log2`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log2.html) | :material-check: |  |
| [`pyspark.sql.functions.lower`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lower.html) |   |  |
| [`pyspark.sql.functions.lpad`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lpad.html) |   |  |
| [`pyspark.sql.functions.ltrim`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ltrim.html) |   |  |
| [`pyspark.sql.functions.make_date`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_date.html) |   |  |
| [`pyspark.sql.functions.make_dt_interval`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_dt_interval.html) |   |  |
| [`pyspark.sql.functions.make_interval`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_interval.html) |   |  |
| [`pyspark.sql.functions.make_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_timestamp.html) |   |  |
| [`pyspark.sql.functions.make_timestamp_ltz`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_timestamp_ltz.html) |   |  |
| [`pyspark.sql.functions.make_timestamp_ntz`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_timestamp_ntz.html) |   |  |
| [`pyspark.sql.functions.make_ym_interval`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_ym_interval.html) |   |  |
| [`pyspark.sql.functions.map_concat`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_concat.html) |   |  |
| [`pyspark.sql.functions.map_contains_key`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_contains_key.html) |   |  |
| [`pyspark.sql.functions.map_entries`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_entries.html) |   |  |
| [`pyspark.sql.functions.map_filter`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_filter.html) |   |  |
| [`pyspark.sql.functions.map_from_arrays`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_from_arrays.html) |   |  |
| [`pyspark.sql.functions.map_from_entries`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_from_entries.html) |   |  |
| [`pyspark.sql.functions.map_keys`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_keys.html) |   |  |
| [`pyspark.sql.functions.map_values`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_values.html) |   |  |
| [`pyspark.sql.functions.map_zip_with`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_zip_with.html) |   |  |
| [`pyspark.sql.functions.mask`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mask.html) |   |  |
| [`pyspark.sql.functions.max`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html) |   |  |
| [`pyspark.sql.functions.max_by`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max_by.html) |   |  |
| [`pyspark.sql.functions.md5`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.md5.html) |   |  |
| [`pyspark.sql.functions.mean`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mean.html) | :material-check: |  |
| [`pyspark.sql.functions.median`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.median.html) |   |  |
| [`pyspark.sql.functions.min`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min.html) |   |  |
| [`pyspark.sql.functions.min_by`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min_by.html) |   |  |
| [`pyspark.sql.functions.minute`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.minute.html) |   |  |
| [`pyspark.sql.functions.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mode.html) |   |  |
| [`pyspark.sql.functions.monotonically_increasing_id`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.monotonically_increasing_id.html) |   |  |
| [`pyspark.sql.functions.month`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.month.html) |   |  |
| [`pyspark.sql.functions.months`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.months.html) |   |  |
| [`pyspark.sql.functions.months_between`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.months_between.html) |   |  |
| [`pyspark.sql.functions.named_struct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.named_struct.html) |   |  |
| [`pyspark.sql.functions.nanvl`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nanvl.html) |   |  |
| [`pyspark.sql.functions.negate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.negate.html) | :material-check: |  |
| [`pyspark.sql.functions.negative`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.negative.html) | :material-check: |  |
| [`pyspark.sql.functions.next_day`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.next_day.html) |   |  |
| [`pyspark.sql.functions.now`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.now.html) |   |  |
| [`pyspark.sql.functions.nth_value`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nth_value.html) |   |  |
| [`pyspark.sql.functions.ntile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ntile.html) |   |  |
| [`pyspark.sql.functions.nullif`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nullif.html) |   |  |
| [`pyspark.sql.functions.nvl`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nvl.html) |   |  |
| [`pyspark.sql.functions.nvl2`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nvl2.html) |   |  |
| [`pyspark.sql.functions.octet_length`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.octet_length.html) |   |  |
| [`pyspark.sql.functions.overlay`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.overlay.html) |   |  |
| [`pyspark.sql.functions.parse_url`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.parse_url.html) |   |  |
| [`pyspark.sql.functions.percent_rank`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percent_rank.html) |   |  |
| [`pyspark.sql.functions.percentile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile.html) |   |  |
| [`pyspark.sql.functions.percentile_approx`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html) |   |  |
| [`pyspark.sql.functions.pi`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pi.html) | :material-check: |  |
| [`pyspark.sql.functions.pmod`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pmod.html) |   |  |
| [`pyspark.sql.functions.posexplode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.posexplode.html) |   |  |
| [`pyspark.sql.functions.posexplode_outer`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.posexplode_outer.html) |   |  |
| [`pyspark.sql.functions.position`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.position.html) |   |  |
| [`pyspark.sql.functions.positive`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.positive.html) |   |  |
| [`pyspark.sql.functions.pow`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pow.html) |   |  |
| [`pyspark.sql.functions.power`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.power.html) |   |  |
| [`pyspark.sql.functions.printf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.printf.html) |   |  |
| [`pyspark.sql.functions.product`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.product.html) |   |  |
| [`pyspark.sql.functions.quarter`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.quarter.html) |   |  |
| [`pyspark.sql.functions.radians`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.radians.html) | :material-check: |  |
| [`pyspark.sql.functions.raise_error`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.raise_error.html) |   |  |
| [`pyspark.sql.functions.rand`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rand.html) |   |  |
| [`pyspark.sql.functions.randn`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.randn.html) |   |  |
| [`pyspark.sql.functions.rank`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html) |   |  |
| [`pyspark.sql.functions.reduce`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.reduce.html) |   |  |
| [`pyspark.sql.functions.reflect`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.reflect.html) |   |  |
| [`pyspark.sql.functions.regexp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp.html) |   |  |
| [`pyspark.sql.functions.regexp_count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_count.html) |   |  |
| [`pyspark.sql.functions.regexp_extract`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html) |   |  |
| [`pyspark.sql.functions.regexp_extract_all`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract_all.html) |   |  |
| [`pyspark.sql.functions.regexp_instr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_instr.html) |   |  |
| [`pyspark.sql.functions.regexp_like`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_like.html) |   |  |
| [`pyspark.sql.functions.regexp_replace`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html) |   |  |
| [`pyspark.sql.functions.regexp_substr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_substr.html) |   |  |
| [`pyspark.sql.functions.regr_avgx`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_avgx.html) |   |  |
| [`pyspark.sql.functions.regr_avgy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_avgy.html) |   |  |
| [`pyspark.sql.functions.regr_count`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_count.html) |   |  |
| [`pyspark.sql.functions.regr_intercept`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_intercept.html) |   |  |
| [`pyspark.sql.functions.regr_r2`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_r2.html) |   |  |
| [`pyspark.sql.functions.regr_slope`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_slope.html) |   |  |
| [`pyspark.sql.functions.regr_sxx`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_sxx.html) |   |  |
| [`pyspark.sql.functions.regr_sxy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_sxy.html) |   |  |
| [`pyspark.sql.functions.regr_syy`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regr_syy.html) |   |  |
| [`pyspark.sql.functions.repeat`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.repeat.html) |   |  |
| [`pyspark.sql.functions.replace`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.replace.html) |   |  |
| [`pyspark.sql.functions.reverse`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.reverse.html) |   |  |
| [`pyspark.sql.functions.right`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.right.html) |   |  |
| [`pyspark.sql.functions.rint`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rint.html) |   |  |
| [`pyspark.sql.functions.rlike`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rlike.html) |   |  |
| [`pyspark.sql.functions.round`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.round.html) | :material-check: |  |
| [`pyspark.sql.functions.row_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html) |   |  |
| [`pyspark.sql.functions.rpad`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rpad.html) |   |  |
| [`pyspark.sql.functions.rtrim`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rtrim.html) |   |  |
| [`pyspark.sql.functions.schema_of_csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_csv.html) |   |  |
| [`pyspark.sql.functions.schema_of_json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_json.html) |   |  |
| [`pyspark.sql.functions.sec`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sec.html) | :material-check: |  |
| [`pyspark.sql.functions.second`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.second.html) |   |  |
| [`pyspark.sql.functions.sentences`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sentences.html) |   |  |
| [`pyspark.sql.functions.sequence`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sequence.html) |   |  |
| [`pyspark.sql.functions.session_window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.session_window.html) |   |  |
| [`pyspark.sql.functions.sha`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sha.html) |   |  |
| [`pyspark.sql.functions.sha1`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sha1.html) |   |  |
| [`pyspark.sql.functions.sha2`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sha2.html) |   |  |
| [`pyspark.sql.functions.shiftLeft`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftLeft.html) |   |  |
| [`pyspark.sql.functions.shiftRight`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftRight.html) |   |  |
| [`pyspark.sql.functions.shiftRightUnsigned`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftRightUnsigned.html) |   |  |
| [`pyspark.sql.functions.shiftleft`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftleft.html) |   |  |
| [`pyspark.sql.functions.shiftright`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftright.html) |   |  |
| [`pyspark.sql.functions.shiftrightunsigned`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftrightunsigned.html) |   |  |
| [`pyspark.sql.functions.shuffle`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shuffle.html) |   |  |
| [`pyspark.sql.functions.sign`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sign.html) | :material-check: |  |
| [`pyspark.sql.functions.signum`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.signum.html) | :material-check: |  |
| [`pyspark.sql.functions.sin`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sin.html) | :material-check: |  |
| [`pyspark.sql.functions.sinh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sinh.html) | :material-check: |  |
| [`pyspark.sql.functions.size`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html) |   |  |
| [`pyspark.sql.functions.skewness`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.skewness.html) |   |  |
| [`pyspark.sql.functions.slice`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.slice.html) |   |  |
| [`pyspark.sql.functions.some`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.some.html) |   |  |
| [`pyspark.sql.functions.sort_array`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sort_array.html) |   |  |
| [`pyspark.sql.functions.soundex`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.soundex.html) |   |  |
| [`pyspark.sql.functions.spark_partition_id`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html) |   |  |
| [`pyspark.sql.functions.split`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html) |   |  |
| [`pyspark.sql.functions.split_part`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split_part.html) |   |  |
| [`pyspark.sql.functions.sqrt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sqrt.html) | :material-check: |  |
| [`pyspark.sql.functions.stack`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stack.html) |   |  |
| [`pyspark.sql.functions.startswith`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.startswith.html) |   |  |
| [`pyspark.sql.functions.std`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.std.html) |   |  |
| [`pyspark.sql.functions.stddev`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev.html) |   |  |
| [`pyspark.sql.functions.stddev_pop`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev_pop.html) |   |  |
| [`pyspark.sql.functions.stddev_samp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev_samp.html) |   |  |
| [`pyspark.sql.functions.str_to_map`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.str_to_map.html) |   |  |
| [`pyspark.sql.functions.struct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.struct.html) |   |  |
| [`pyspark.sql.functions.substr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substr.html) |   |  |
| [`pyspark.sql.functions.substring`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html) |   |  |
| [`pyspark.sql.functions.substring_index`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring_index.html) |   |  |
| [`pyspark.sql.functions.sum`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html) |   |  |
| [`pyspark.sql.functions.sumDistinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sumDistinct.html) |   |  |
| [`pyspark.sql.functions.sum_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum_distinct.html) |   |  |
| [`pyspark.sql.functions.tan`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.tan.html) | :material-check: |  |
| [`pyspark.sql.functions.tanh`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.tanh.html) | :material-check: |  |
| [`pyspark.sql.functions.timestamp_micros`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_micros.html) |   |  |
| [`pyspark.sql.functions.timestamp_millis`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_millis.html) |   |  |
| [`pyspark.sql.functions.timestamp_seconds`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_seconds.html) |   |  |
| [`pyspark.sql.functions.toDegrees`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.toDegrees.html) |   |  |
| [`pyspark.sql.functions.toRadians`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.toRadians.html) |   |  |
| [`pyspark.sql.functions.to_binary`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_binary.html) |   |  |
| [`pyspark.sql.functions.to_char`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_char.html) |   |  |
| [`pyspark.sql.functions.to_csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_csv.html) |   |  |
| [`pyspark.sql.functions.to_date`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html) |   |  |
| [`pyspark.sql.functions.to_json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html) |   |  |
| [`pyspark.sql.functions.to_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_number.html) |   |  |
| [`pyspark.sql.functions.to_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html) | :material-check: |  |
| [`pyspark.sql.functions.to_timestamp_ltz`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html) |   |  |
| [`pyspark.sql.functions.to_timestamp_ntz`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html) |   |  |
| [`pyspark.sql.functions.to_unix_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_unix_timestamp.html) |   |  |
| [`pyspark.sql.functions.to_utc_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_utc_timestamp.html) |   |  |
| [`pyspark.sql.functions.to_varchar`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_varchar.html) |   |  |
| [`pyspark.sql.functions.transform`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html) |   |  |
| [`pyspark.sql.functions.transform_keys`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform_keys.html) |   |  |
| [`pyspark.sql.functions.transform_values`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform_values.html) |   |  |
| [`pyspark.sql.functions.translate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.translate.html) |   |  |
| [`pyspark.sql.functions.trim`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html) |   |  |
| [`pyspark.sql.functions.trunc`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trunc.html) |   |  |
| [`pyspark.sql.functions.try_add`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_add.html) |   |  |
| [`pyspark.sql.functions.try_aes_decrypt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_aes_decrypt.html) |   |  |
| [`pyspark.sql.functions.try_avg`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_avg.html) |   |  |
| [`pyspark.sql.functions.try_divide`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_divide.html) |   |  |
| [`pyspark.sql.functions.try_element_at`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_element_at.html) |   |  |
| [`pyspark.sql.functions.try_multiply`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_multiply.html) |   |  |
| [`pyspark.sql.functions.try_subtract`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_subtract.html) |   |  |
| [`pyspark.sql.functions.try_sum`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_sum.html) |   |  |
| [`pyspark.sql.functions.try_to_binary`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_binary.html) |   |  |
| [`pyspark.sql.functions.try_to_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_number.html) |   |  |
| [`pyspark.sql.functions.try_to_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_timestamp.html) |   |  |
| [`pyspark.sql.functions.typeof`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.typeof.html) |   |  |
| [`pyspark.sql.functions.ucase`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ucase.html) |   |  |
| [`pyspark.sql.functions.udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html) |   |  |
| [`pyspark.sql.functions.udtf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udtf.html) |   |  |
| [`pyspark.sql.functions.unbase64`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unbase64.html) |   |  |
| [`pyspark.sql.functions.unhex`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unhex.html) |   |  |
| [`pyspark.sql.functions.unix_date`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_date.html) |   |  |
| [`pyspark.sql.functions.unix_micros`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_micros.html) |   |  |
| [`pyspark.sql.functions.unix_millis`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_millis.html) |   |  |
| [`pyspark.sql.functions.unix_seconds`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_seconds.html) |   |  |
| [`pyspark.sql.functions.unix_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_timestamp.html) |   |  |
| [`pyspark.sql.functions.unwrap_udt`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unwrap_udt.html) |   |  |
| [`pyspark.sql.functions.upper`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.upper.html) |   |  |
| [`pyspark.sql.functions.url_decode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.url_decode.html) |   |  |
| [`pyspark.sql.functions.url_encode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.url_encode.html) |   |  |
| [`pyspark.sql.functions.user`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.user.html) |   |  |
| [`pyspark.sql.functions.var_pop`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.var_pop.html) |   |  |
| [`pyspark.sql.functions.var_samp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.var_samp.html) |   |  |
| [`pyspark.sql.functions.variance`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.variance.html) |   |  |
| [`pyspark.sql.functions.version`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.version.html) | :material-check: |  |
| [`pyspark.sql.functions.weekday`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekday.html) |   |  |
| [`pyspark.sql.functions.weekofyear`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekofyear.html) |   |  |
| [`pyspark.sql.functions.when`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html) |   |  |
| [`pyspark.sql.functions.width_bucket`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.width_bucket.html) |   |  |
| [`pyspark.sql.functions.window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window.html) |   |  |
| [`pyspark.sql.functions.window_time`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window_time.html) |   |  |
| [`pyspark.sql.functions.xpath`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath.html) |   |  |
| [`pyspark.sql.functions.xpath_boolean`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_boolean.html) |   |  |
| [`pyspark.sql.functions.xpath_double`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_double.html) |   |  |
| [`pyspark.sql.functions.xpath_float`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_float.html) |   |  |
| [`pyspark.sql.functions.xpath_int`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_int.html) |   |  |
| [`pyspark.sql.functions.xpath_long`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_long.html) |   |  |
| [`pyspark.sql.functions.xpath_number`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_number.html) |   |  |
| [`pyspark.sql.functions.xpath_short`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_short.html) |   |  |
| [`pyspark.sql.functions.xpath_string`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xpath_string.html) |   |  |
| [`pyspark.sql.functions.xxhash64`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xxhash64.html) |   |  |
| [`pyspark.sql.functions.year`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html) |   |  |
| [`pyspark.sql.functions.years`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.years.html) |   |  |
| [`pyspark.sql.functions.zip_with`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.zip_with.html) |   |  |

