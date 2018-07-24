package com.ifeng.appitemsdb

import java.util.Properties

import com.datastax.spark.connector.cql.CassandraConnector
import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._

object MultiAppItemCassan2 {
  def main(args: Array[String]): Unit = {
    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")
    val connectionProperties = new Properties()

    val conf = new SparkConf()
//      .setMaster("spark://10.90.9.111:7077")
//      .setAppName("AppitemCassan")
      //      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.cassandra.connection.host", "10.90.13.101")
      .set("spark.executor.memory", "20g")
      .set("spark.cores.max", "200")
      .setJars(alljars)
    val sc = new SparkContext("spark://10.90.9.111:7077", "test7", conf)
    twoClusterExample(sc)
  }

  def twoClusterExample(sc: SparkContext) = {
    val connectorToClusterOne = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "10.90.13.101"))
    val connectorToClusterTwo = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", "10.80.17.155"))

    val rddFromClusterOne = {
      // Sets connectorToClusterOne as default connection for everything in this code block
      implicit val c = connectorToClusterOne
      sc.cassandraTable("ikv", "appitemdb").select("key","value")
    }

    {
      //Sets connectorToClusterTwo as the default connection for everything in this code block
      implicit val c = connectorToClusterTwo
      rddFromClusterOne.saveToCassandra("groups", "test7")
    }
  }
}
