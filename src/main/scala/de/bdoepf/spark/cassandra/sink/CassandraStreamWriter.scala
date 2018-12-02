package de.bdoepf.spark.cassandra.sink

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.CassandraDataWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType


class CassandraStreamWriter(connector: CassandraConnector,
                            keyspaceName: String,
                            tableName: String,
                            schema: StructType) extends StreamWriter {
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {

  }

  override def createWriterFactory(): DataWriterFactory[Row] = new CassandraStreamDataWriterFactory(
    connector,
    keyspaceName,
    tableName,
    schema)
}

class CassandraStreamDataWriterFactory(connector: CassandraConnector,
                                       keyspaceName: String,
                                       tableName: String,
                                       schema: StructType) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] =
    new CassandraDataWriter(connector, keyspaceName, tableName, schema)
}