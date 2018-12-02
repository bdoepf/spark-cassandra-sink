name := "spark-cassandra-sink"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "io.netty" % "netty-all" % "4.1.17.Final"
