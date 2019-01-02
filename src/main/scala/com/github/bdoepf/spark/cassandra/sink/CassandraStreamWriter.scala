package com.github.bdoepf.spark.cassandra.sink

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.writer.CassandraDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


class CassandraStreamWriter(connector: CassandraConnector,
                            columns: IndexedSeq[ColumnRef],
                            tableDef: TableDef,
                            schema: StructType) extends StreamWriter {
  private val log = LoggerFactory.getLogger(this.getClass.getName)
  log.debug(s"Initializing ${this.getClass.getSimpleName}")

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {

  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new CassandraStreamDataWriterFactory(
    connector,
    columns,
    tableDef,
    schema)
}

class CassandraStreamDataWriterFactory(connector: CassandraConnector,
                                       columns: IndexedSeq[ColumnRef],
                                       tableDef: TableDef,
                                       schema: StructType) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] =
    new CassandraDataWriter(connector, columns, tableDef, schema)
}