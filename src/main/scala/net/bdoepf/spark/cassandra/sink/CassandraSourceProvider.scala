package net.bdoepf.spark.cassandra.sink

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.{ColumnRef, ColumnSelector, SomeColumns}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class CassandraSourceProvider extends DataSourceV2 with StreamWriteSupport with DataSourceRegister {
  private val log = LoggerFactory.getLogger(this.getClass.getName)
  log.info(s"Initializing ${this.getClass.getSimpleName}")

  final val TableNameConfig = "table"
  final val KeyspaceNameConfig = "keyspace"

  override def shortName(): String = "cassandra-streaming"

  override def createStreamWriter(queryId: String,
                                  schema: StructType,
                                  mode: OutputMode,
                                  options: DataSourceOptions): StreamWriter = {

    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    require(session.isDefined)

    if (mode != OutputMode.Append()) {
      throw new IllegalStateException("Hive Streaming only supports output with Append mode")
    }

    val parameters = options.asMap().asScala
    val keyspaceName = getRequiredParameter(KeyspaceNameConfig, parameters)
    val tableName = getRequiredParameter(TableNameConfig, parameters)

    val cassandraConnector = CassandraConnector(session.get.sharedState.sparkContext.getConf)

    val tableDef = try {
      Schema.tableFromCassandra(cassandraConnector, keyspaceName, tableName)
    } catch {
      case e: Exception => throw new IllegalStateException("Cannot find destination table in " +
        "cassandra, please create table at first", e)
    }

    // Columns that cannot actually be written to because they represent virtual endpoints
    val InternalColumns = Set("solr_query")

    val rowColumnRefs = schema.fields.map(_.name: ColumnRef)
    val columns: ColumnSelector = SomeColumns(rowColumnRefs: _*)

    val selectedColumns: IndexedSeq[ColumnRef] = try {
      columns
        .selectFrom(tableDef)
        .filter(col => !InternalColumns.contains(col.columnName))
    } catch {
      case _: Exception => throw new IllegalStateException(s"Not all columns from input source schema $schema could " +
        s"be found in cassandra table definition.")
    }

    new CassandraStreamWriter(cassandraConnector, selectedColumns, tableDef)
  }

  private def getRequiredParameter(parameterKey: String, parameters: mutable.Map[String, String]) = {
    parameters.getOrElse(parameterKey, throw new IllegalArgumentException(
      s"Cassandra option '$parameterKey' must be specified for cassandra sink"
    ))
  }

}
