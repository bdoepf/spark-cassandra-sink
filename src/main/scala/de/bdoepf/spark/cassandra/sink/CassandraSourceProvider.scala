package de.bdoepf.spark.cassandra.sink

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

class CassandraSourceProvider extends DataSourceV2 with StreamWriteSupport with DataSourceRegister {
  final val TableNameConfig = "table"
  final val KeyspaceNameConfig = "keyspace"

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
    val keyspace = getRequiredParameter(KeyspaceNameConfig, parameters)
    val table = getRequiredParameter(TableNameConfig, parameters)


    val destTable = try {
      // TODO check if table exists
//      session.get.sharedState.externalCatalog.getTable(
//        localHiveOptions.dbName, localHiveOptions.tableName)
    } catch {
      case e: Exception => throw new IllegalStateException("Cannot find destination table in " +
        "cassandra, please create table at first", e)
    }
    val cassandraConnector = CassandraConnector(session.get.sharedState.sparkContext.getConf)

    // TODO check schema
//    if (schema.map(_.name).toSet != destSchema.map(_.name).toSet) {
//      throw new IllegalStateException(s"Schema $schema transformed from input source is different" +
//        s" from schema $destSchema for the destination table")
//    }

    new CassandraStreamWriter(cassandraConnector, keyspace, table, schema)
  }

  private def getRequiredParameter(parameterKey: String, parameters: mutable.Map[String, String]) = {
    parameters.getOrElse(parameterKey, throw new IllegalArgumentException(
      s"Cassandra option '$parameterKey' must be specified for cassandra sink"
    ))
  }

  override def shortName(): String = "cassandra-streaming"
}
