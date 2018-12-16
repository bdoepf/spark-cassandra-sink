package bdoepf.spark.cassandra.sink

import java.nio.file.{Files, Path}
import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}

final case class TestItem(id: Int, descr: String)
final case class PartialTestItem(id: Int)
final case class TestItemOptional(id: Int, descr: Option[String])


class CassandraSourceProviderSpec extends WordSpec with Matchers with EmbeddedCassandra with SparkTemplate with BeforeAndAfterEach {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  val tmpDir: Path = Files.createTempDirectory("spark-cassandra-sink-test")
  tmpDir.toFile.deleteOnExit()

  System.setProperty("test.cassandra.version", "3.2")

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)


  override protected def afterEach(): Unit = {
    truncateTable()
    FileUtils.cleanDirectory(tmpDir.toFile)

    super.afterEach()
  }

  lazy val spark: SparkSession = sparkSession
  implicit lazy val sqlContext: SQLContext = spark.sqlContext

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

  "cassandra structured streaming sink" should {
    "insert rows into cassandra" when {
      "source has identical schema like the cassandra table" in {
        val sourceStream = MemoryStream[TestItem]

        val query = sourceStream
          .toDS()
          .writeStream
          .format("cassandra-streaming")
          .option("keyspace", ksName)
          .option("table", tableName)
          .option("checkpointLocation", tmpDir.toString)
          .queryName(UUID.randomUUID().toString)
          .start()

        val testItems = Range(0, 5).map(x => TestItem(x, s"description of $x"))
        sourceStream.addData(testItems)
        println(testItems)

        query.processAllAvailable()
        query.stop()

        val results = spark.sparkContext.cassandraTable[TestItem](ksName, tableName).collect()
        results.sortBy(_.id) should contain theSameElementsAs testItems.toArray.sortBy(_.id)
      }
      "source has less columns than the cassandra table but all mandatory columns (partition) are available" in {
        val sourceStream = MemoryStream[PartialTestItem]

        val query = sourceStream
          .toDS()
          .writeStream
          .format("cassandra-streaming")
          .option("keyspace", ksName)
          .option("table", tableName)
          .option("checkpointLocation", tmpDir.toString)
          .queryName(UUID.randomUUID().toString)
          .start()

        val testItems = Range(0, 5).map(PartialTestItem)
        sourceStream.addData(testItems)

        query.processAllAvailable()
        query.stop()

        val results = spark.sparkContext.cassandraTable[TestItemOptional](ksName, tableName).collect()
        results should contain theSameElementsAs testItems.map(x => TestItemOptional(x.id, None))
      }
    }
  }
}
