name := "LSJ2018"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkV = "2.2.0"
  val cassandraV = "2.0.5"
  Seq(
    "org.apache.spark" %% "spark-hive" % "2.2.0",
    "mysql" % "mysql-connector-java" % "6.0.6",
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-hive" % sparkV,
    "com.datastax.spark" %% "spark-cassandra-connector" % cassandraV,
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"
  )
}

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"