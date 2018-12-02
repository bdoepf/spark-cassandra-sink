//package org.apache.spark.sql.streaming
//
//import org.apache.spark.sql.execution.datasources.DataSource
//import org.apache.spark.sql.sources.v2.StreamWriteSupport
//
//class CassandraSink[T](dsw: DataStreamWriter[T]) {
//  private val df = ds.toDF()
//
//  def startWithoutCheckpoint(source: String, outputMode: OutputMode, trigger: Trigger, extraOptions: Map[String, String]): StreamingQuery = {
//    val ds = DataSource.lookupDataSource(source, df.sparkSession.sessionState.conf)
//    val disabledSources = df.sparkSession.sqlContext.conf.disabledV2StreamingWriters.split(",")
//    val sink = ds.newInstance() match {
//      case w: StreamWriteSupport if !disabledSources.contains(w.getClass.getCanonicalName) => w
//      case _ =>
//        val ds = DataSource(
//          df.sparkSession,
//          className = source,
//          options = extraOptions.toMap,
//          partitionColumns = Nil)
//        ds.createSink(outputMode)
//    }
//
//    df.sparkSession.sessionState.streamingQueryManager.startQuery(
//      extraOptions.get("queryName"),
//      extraOptions.get("checkpointLocation"),
//      df,
//      extraOptions.toMap,
//      sink,
//      outputMode,
//      useTempCheckpointLocation = source == "console",
//      recoverFromCheckpointLocation = true,
//      trigger = trigger)
//
//  }
//
//}
//object CassandraSink {
//  implicit def starting[T](ds: Dataset[T]): CassandraSink[T] = new CassandraSink[T](ds)
//}