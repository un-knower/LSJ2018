package libsvm

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.{SaveMode, SparkSession}
//import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}

object T1T2_libsvm_test2 {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("Uimge_Insert_sql")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory","10g")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_allvalues", "keyspace" -> "uimg"))
      .load()
      .select("uid", "t1")

//    df1.map ( row => row.getString("uid"))
//
//    query1.write
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "uimge_allvalues", "keyspace" -> "uimg"))
//      .mode(SaveMode.Append)
//      .save()
  }
}
