package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

final case class MyItem(id: Int, descr: String)

class CassandraDataWriterSpec extends FlatSpec with Matchers {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", 9042)
    .getOrCreate()

  import spark.implicits._

  "CassandraRowInserter" should "insert spark Rows into cassandra table" in {
    import org.apache.spark.sql.Encoders
    val mySchema = Encoders.product[MyItem].schema
    // TODO create schema from case class
    val ds = spark.createDataset(List(MyItem(5, "My Item with id 5")))
    val row = ds.toDF().collect().head

    val connector = CassandraConnector(spark.sparkContext.getConf)
    val inserter = new CassandraDataWriter(connector, "my_keyspace", "my_table", mySchema)

    inserter.write(row)
  }



}

object ManualTest {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", 9042)
      .getOrCreate()
    import spark.implicits._

    // nc -lk 9999
    val host = "localhost"
    val port = "9999"
    val socket = spark.readStream
      .format("socket")
      .options(Map("host" -> host, "port" -> port))
      .load()
      .as[String]

    val query = socket.map { s =>
      val records = s.split(",")
      assert(records.length >= 2)
      (records(0).toInt, records(1))
    }
      .selectExpr("_1 as id", "_2 as descr")
      .writeStream
      //.format("de.bdoepf.spark.cassandra.sink.CassandraSourceProvider")
      .format("cassandra-streaming")
      .option("checkpointLocation", "/tmp/demo-checkpoint")
      .option("keyspace", "my_keyspace")
      .option("table", "my_table")
      .queryName("socket-cassandra-streaming")
      .start()

    query.awaitTermination()
  }
}