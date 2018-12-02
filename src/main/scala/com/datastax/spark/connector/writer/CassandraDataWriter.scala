package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.driver.core._
import com.datastax.spark.connector.{CassandraRow, ColumnRef, SomeColumns}
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

case object CassandraStreamCommitMessage extends WriterCommitMessage


class CassandraDataWriter(connector: CassandraConnector, keyspaceName: String, tableName: String, schema: StructType, writeConf: WriteConf = WriteConf())
  extends DataWriter[Row] {
  // private val optionColumns: Seq[ColumnDef] = writeConf.optionsAsColumns(keyspaceName, tableName)
  private val tableDef: TableDef = Schema.tableFromCassandra(connector, keyspaceName, tableName)

  // Columns that cannot actually be written to because they represent virtual endpoints
  private val InternalColumns = Set("solr_query")

  private val rowColumnRefs = schema.fields.map(_.name: ColumnRef)
  private val columns = SomeColumns(rowColumnRefs: _*)

  private val selectedColumns: IndexedSeq[ColumnRef] = columns
    .selectFrom(tableDef)
    .filter(col => !InternalColumns.contains(col.columnName))

  private val rowWriter: RowWriter[Row] = implicitly[RowWriterFactory[Row]].rowWriter(
    tableDef,
    selectedColumns)

  // Table writer
  private val tw: TableWriter[CassandraRow] = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
  private val queryTemplate = tw.queryTemplateUsingInsert

  connector.withSessionDo { session =>
    prepareStatement(tw, queryTemplate, session).setConsistencyLevel(writeConf.consistencyLevel)
  }

  override def write(record: Row) = {
    connector.withSessionDo { session =>
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      val stmt = prepareStatement(tw, queryTemplate, session).setConsistencyLevel(writeConf.consistencyLevel)

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = false)

      val boundStatement = boundStmtBuilder.bind(record)
      session.executeAsync(boundStatement)
    }
  }

  private def prepareStatement(tw: TableWriter[CassandraRow], queryTemplate: String, session: Session): PreparedStatement = {
    try {
      val stmt = new SimpleStatement(queryTemplate)
      stmt.setIdempotent(tw.isIdempotent)
      // Cache prep statements to avoid re-preparing
      PreparedStatementCache.prepareStatement(session, stmt)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  override def commit(): WriterCommitMessage = CassandraStreamCommitMessage

  override def abort(): Unit = {
    //
  }
}
