package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.datastax.spark.connector.{ColumnRef, _}
import org.apache.spark.sql.SparkSession
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

  import spark.implicits._

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
    val mySchema = Encoders.product[TestItem].schema

    val testData = List(TestItem(5, "My Item with id 5"))
    val ds = spark.createDataset(testData)
    val row = ds.toDF().collect().head

    val connector = CassandraConnector(spark.sparkContext.getConf)
    val rowColumnRefs = mySchema.fields.map(_.name: ColumnRef).toIndexedSeq
    val tableDef = Schema.tableFromCassandra(connector, ksName, tableName)

    val inserter = new CassandraDataWriter(connector, rowColumnRefs, tableDef)
    inserter.write(row)

    val results = sc.cassandraTable[TestItem](ksName, tableName).collect()
    results should contain theSameElementsAs testData
  }
}

//
//object ManualTest {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .config("spark.cassandra.connection.host", "localhost")
//      .config("spark.cassandra.connection.port", 9042)
//      .getOrCreate()
//    import spark.implicits._
//
//    // nc -lk 9999
//    val host = "localhost"
//    val port = "9999"
//    val socket = spark.readStream
//      .format("socket")
//      .options(Map("host" -> host, "port" -> port))
//      .load()
//      .as[String]
//
//    val query = socket.map { s =>
//      val records = s.split(",")
//      assert(records.length >= 2)
//      (records(0).toInt, records(1))
//    }
//      .selectExpr("_1 as id", "_2 as descr")
//      .writeStream
//      //.format("de.bdoepf.spark.cassandra.sink.CassandraSourceProvider")
//      .format("cassandra-streaming")
//      .option("checkpointLocation", "/tmp/demo-checkpoint")
//      .option("keyspace", "my_keyspace")
//      .option("table", "my_table")
//      .queryName("socket-cassandra-streaming")
//      .start()
//
//    query.awaitTermination()
//  }
//}