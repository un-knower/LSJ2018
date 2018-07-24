package com.ifeng.appitemsdb

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.SparkContext
import libsvm.PackageChecker
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import java.util.Properties
//import com.datastax.spark.connector.cql.CassandraConnectorConf
//import com.datastax.spark.connector.rdd.ReadConf


object MultiAppItemCassan {
  def main(args: Array[String]): Unit =  {
    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")
//    val connectionProperties = new Properties()

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("AppitemCassan")
//      .set("spark.cassandra.connection.host", "10.80.17.155")
//      .set("spark.cassandra.connection.host", "10.90.13.101")
      .set("spark.executor.memory","30g")
      .set("spark.cores.max", "200")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
//    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    // set params for the particular cluster
    spark.setCassandraConf("Cluster1",
      CassandraConnectorConf.ConnectionHostParam
        .option("10.90.13.101,10.90.13.102,10.90.13.103,10.90.13.104,10.90.13.105"))
//      ++ CassandraConnectorConf.ConnectionPortParam.option(12345))
    spark.setCassandraConf("Cluster2",
      CassandraConnectorConf.ConnectionHostParam
      .option("10.80.17.155,10.80.18.155,10.80.19.155,10.80.20.155,10.80.21.155,10.80.22.155,10.80.23.155,10.80.24.155,10.80.25.155"))

    import spark.implicits._
    val appitemdf =  spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "appitemdb", "keyspace" -> "ikv" , "cluster" -> "Cluster1"))
      .load()
      .filter("length(key)<20")
      .select($"key",
        expr("regexp_extract(json_parse(value),'$.category[0]')").alias("value"),
        expr("regexp_extract(json_parse(value),'$.title')").alias("title"),
        expr("regexp_extract(json_parse(value),'$.subid')").alias("subid"))

    appitemdf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "acticletest1", "keyspace" -> "groups" , "cluster" -> "Cluster2"))
      .mode(SaveMode.Append)
      .save()
  }
}
