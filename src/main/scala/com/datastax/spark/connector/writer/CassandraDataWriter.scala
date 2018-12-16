package com.datastax.spark.connector.writer

import java.io.IOException

import com.datastax.driver.core._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.{CassandraRow, ColumnRef, SomeColumns}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.slf4j.{Logger, LoggerFactory}

case object CassandraStreamCommitMessage extends WriterCommitMessage


class CassandraDataWriter(connector: CassandraConnector,
                          columns: IndexedSeq[ColumnRef],
                          tableDef: TableDef)
  extends DataWriter[Row] {
  private val log = LoggerFactory.getLogger(this.getClass.getName)
  log.info(s"Initializing ${this.getClass.getSimpleName}")

  private val keyspaceName: String = tableDef.keyspaceName
  private val tableName: String = tableDef.tableName
  private val rowWriter: RowWriter[Row] = implicitly[RowWriterFactory[Row]].rowWriter(
    tableDef,
    columns)

  private val writeConf = WriteConf()
  // Table writer
  private val tw: TableWriter[CassandraRow] = TableWriter(connector, keyspaceName, tableName, SomeColumns(columns: _*), writeConf)

  private val insertTemplate = tw.queryTemplateUsingInsert
  private val isIdempotent = tw.isIdempotent
  private val consistencyLevel: ConsistencyLevel = writeConf.consistencyLevel
  connector.withSessionDo { session =>
    prepareStatement(insertTemplate, session, isIdempotent).setConsistencyLevel(consistencyLevel)
  }

  override def write(record: Row): Unit = {
    log.info(s"Writing row $record")
    connector.withSessionDo { session =>
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      val stmt = prepareStatement(insertTemplate, session, isIdempotent).setConsistencyLevel(consistencyLevel)

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = false)

      val boundStatement = boundStmtBuilder.bind(record)
      session.executeAsync(boundStatement)
    }
  }

  private def prepareStatement(queryTemplate: String, session: Session, isIdempotent: Boolean): PreparedStatement = {
    try {
      val stmt = new SimpleStatement(queryTemplate)
      stmt.setIdempotent(isIdempotent)
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
