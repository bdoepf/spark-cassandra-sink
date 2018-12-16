package bdoepf.spark.cassandra.sink

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.writer.CassandraDataWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.slf4j.LoggerFactory


class CassandraStreamWriter(connector: CassandraConnector,
                            columns: IndexedSeq[ColumnRef],
                            tableDef: TableDef) extends StreamWriter {
  private val log = LoggerFactory.getLogger(this.getClass.getName)
  log.info(s"Initializing ${this.getClass.getSimpleName}")

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {

  }

  override def createWriterFactory(): DataWriterFactory[Row] = new CassandraStreamDataWriterFactory(
    connector,
    columns, tableDef)
}

class CassandraStreamDataWriterFactory(connector: CassandraConnector,
                                       columns: IndexedSeq[ColumnRef],
                                       tableDef: TableDef) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] =
    new CassandraDataWriter(connector, columns, tableDef)
}