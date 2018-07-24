package uimgeLevelInsert

import libsvm.PackageChecker
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object UimgeALevelInsert {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("Uimge_ALevel_insert_test")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "30g")
      .set("spark.cores.max", "300")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_alevel", "keyspace" -> "groups"))
      .load()

    df1.createOrReplaceTempView("table")
    val query1 = spark.sql("select uid, a601, a602, a603, a604, a605, a606," +
      "a607, a608, a609, a701, a702, a703, a704," +
      "a705, a706, a707, a801, a802, a803, a804," +
      "a805, a806, a901, a902, a903, a904, a905, a906, a907," +
      "a1001, a1002, a1003, a1004, a1005, a1006, a1007," +
      "a1101, a1102, a1103, a1104, a1105, a1106, a1107, a1108," +
      "a1201, a1202, a1203, a1204, a1205, a1206, a1301," +
      "a1302, a1303, a1304, a1305, a1306, a1307, a1308, a1309, a1401, a1402, a1403," +
      "a1405, a1406, a1407, a1408, a1501, a1502, a1503, a1504, a1601, a1602," +
      "a1603, a1604, a1605, a1606, a1607, a1701, a1702, a1703," +
      "a1801, a1802, a1803, a1804 from table where ((CASE WHEN a601 IS NULL THEN 0 ELSE a601 END) + " +
      "(CASE WHEN a701 IS NULL THEN 0 ELSE a701 END) +" +
      "(CASE WHEN a801 IS NULL THEN 0 ELSE a801 END) +" +
      "(CASE WHEN a901 IS NULL THEN 0 ELSE a901 END) +" +
      "(CASE WHEN a1001 IS NULL THEN 0 ELSE a1001 END) +" +
      "(CASE WHEN a1101 IS NULL THEN 0 ELSE a1101 END) +" +
      "(CASE WHEN a1201 IS NULL THEN 0 ELSE a1201 END) +" +
      "(CASE WHEN a1301 IS NULL THEN 0 ELSE a1301 END) +" +
      "(CASE WHEN a1401 IS NULL THEN 0 ELSE a1401 END) +" +
      "(CASE WHEN a1501 IS NULL THEN 0 ELSE a1501 END) +" +
      "(CASE WHEN a1601 IS NULL THEN 0 ELSE a1601 END) +" +
      "(CASE WHEN a1701 IS NULL THEN 0 ELSE a1701 END) +" +
      "(CASE WHEN a1801 IS NULL THEN 0 ELSE a1801 END))>2")

    query1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "test4", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()
  }
}

