package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.datastax.spark.connector.{ColumnRef, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

final case class TestItem(id: Int, descr: String)


class CassandraDataWriterSpec extends FlatSpec with Matchers with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  System.setProperty("test.cassandra.version", "3.2")

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)


  override protected def afterAll(): Unit = {
    truncateTable()
    super.afterAll()
  }

  lazy val spark: SparkSession = sparkSession

  val ksName = "ks"
  val tableName = "test"

  private def truncateTable() = {
    // create keyspace and table
    connector.withSessionDo { session =>
      session.execute(s"TRUNCATE $ksName.$tableName")
    }
  }

  val connector = CassandraConnector(defaultConf)

  // create keyspace and table
  connector.withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ksName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $ksName.$tableName (id int, descr text, PRIMARY KEY (id))")
  }

  classOf[CassandraDataWriter].getSimpleName should "insert spark Rows into cassandra table" in {
    import org.apache.spark.sql.Encoders
    val encoder = Encoders.product[TestItem]
    val mySchema = encoder.schema

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    val personExprEncoder = encoder.asInstanceOf[ExpressionEncoder[TestItem]]

    val testItem = TestItem(5, "My Item with id 5")
    val testRow = personExprEncoder.toRow(testItem)

    val connector = CassandraConnector(spark.sparkContext.getConf)
    val rowColumnRefs = mySchema.fields.map(_.name: ColumnRef).toIndexedSeq
    val tableDef = Schema.tableFromCassandra(connector, ksName, tableName)

    val inserter = new CassandraDataWriter(connector, rowColumnRefs, tableDef, mySchema)
    inserter.write(testRow)

    val results = sc.cassandraTable[TestItem](ksName, tableName).collect()
    results should contain theSameElementsAs List(testItem)
  }
}
