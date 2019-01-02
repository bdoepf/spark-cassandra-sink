name := "spark-cassandra-sink"
organization := "com.github.bdoepf"

scalaVersion := "2.11.12"

// Spark 2.4+ also support scala 2.12,
// Unfortunately right now com.datastax.spark:spark-cassandra-connector is only available in 2.11
// crossScalaVersions := List("2.12.8", "2.11.12")
val sparkVersion = "2.4.0"

// Pinning the artifact's version to spark's version
version := sparkVersion

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion exclude("io.netty", "netty-all")
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % sparkVersion % "test" excludeAll(
  ExclusionRule("org.slf4j","log4j-over-slf4j"),
  ExclusionRule("org.slf4j","slf4j-log4j12")
)
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.2" % "test" excludeAll(
  ExclusionRule("org.slf4j","log4j-over-slf4j"),
  ExclusionRule("org.slf4j","slf4j-log4j12"),
  ExclusionRule("ch.qos.logback", "logback-classic")
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

//Forking is required for the Embedded Cassandra
fork in Test := true
