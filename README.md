spark-cassandra-sink
============

spark-cassandra-sink is a Spark Structured Streaming Sink for cassandra.
It requires a streaming Dataset/Dataframe and inserts its rows into a cassandra table.

Setup
-----

Add the library in the `build.sbt`:
```scala
libraryDependencies += "net.bdoepf" %% "spark-cassandra-sink" % "2.4.0"
```
The version should be the same like the spark version used.

Configuration
-------------
Configure Spark to be able to connect to the cassandra.
```
spark.cassandra.connection.host=localhost
spark.cassandra.connection.port=9092
```
Spark supports several ways for configuration. Please check [Spark's configuration](https://spark.apache.org/docs/latest/configuration.html) documentation for more detials.
For the full list of possible cassandra configurations in Spark please check the internally used [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) docs.


Usage
-----

Use the Spark Cassandra Sink by passing the format `cassandra-streaming`.

```scala
val source = ... // Streaming Dataset or Dataframe

val query = source
     .writeStream
     .format("cassandra-streaming")
     .option("checkpointLocation", "/tmp/checkpoint")
     .option("keyspace", "my_keyspace")
     .option("table", "my_table")
     .queryName("socket-cassandra-streaming")
     .start()
```
The Spark Cassandra Sink inserts the Dataframe/Dataset's rows into a cassandra table.

When using a streaming Dataset the field names and types of the case class used must match the cassandra's table schema.
The same applies for the Dataframe's schema when using a streaming Dataframe. For mapping between scala and cassandra types please check [spark-cassandra-connector documentation](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/2_loading.md#data-type-conversions).

CheckpointLocation is mandatory like for all Spark Structured Streaming Sink's except MemorySink.

Full example
-----


```scala
// configure spark
val spark = SparkSession
  .builder()
  .config("spark.cassandra.connection.host", "localhost")
  .config("spark.cassandra.connection.port", 9042)
  .master("local[*]")
  .getOrCreate()
 
import spark.implicits._
 
// Run `nc -lk 9999` in a separate terminal before starting the spark structured streaming job
// Create the streaming source
val socket = spark
  .readStream
  .format("socket")
  .options(Map("host" -> "localhost", "port" -> "9999"))
  .load()
  .as[String]

// apply transformations
val transformed = socket.map { s =>
     val records = s.split(",")
     assert(records.length >= 2)
     (records(0).toInt, records(1))
   }.selectExpr("_1 as id", "_2 as description")

// Use Spark Cassandra Sink
val query = transformed
     .writeStream
     .format("cassandra-streaming")
     .option("checkpointLocation", "/tmp/demo-checkpoint")
     .option("keyspace", "my_keyspace")
     .option("table", "my_table")
     .queryName("socket-cassandra-streaming")
     .start()

query.awaitTermination()
```

The example above requires a running cassandra on localhost.
Start cassandra via docker
```bash
docker run --name cassandra -d -p 9042:9042 cassandra:latest
```

Save following CQL commands to a file named `create.cql`:
```
cat <<EOF > create.cql
CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS my_keyspace.my_table (
   id int,
   description text,
   PRIMARY KEY (id));
EOF
```

Copy the `create.cql` file into the docker container and apply it
```
docker cp create.cql cassandra:/tmp/
docker exec cassandra cqlsh --file /tmp/create.cql
```

#### Check if it works
1. Start the netcat server
    ```
    nc -lk 9999
    ```
2. Start the spark job from above
3. Write lines in the following schema in the netcat terminal
    ```
    1,hello
    2,world
    ```
4. Check in another terminal if the rows have been written to cassandra
    ```
    docker exec -it cassandra cqlsh
    > SELECT * FROM my_keyspace.my_table;
    ```
    There should have been inserted two rows:
    ```$xslt
     id | description
    ----+-------------
      1 |       hello
      2 |       world
    ```