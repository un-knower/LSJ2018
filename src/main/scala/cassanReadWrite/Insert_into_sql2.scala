package libsvm

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.{SaveMode, SparkSession}
//import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}

object Insert_into_sql2 {
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
      .set("spark.executor.memory","20g")
      .set("spark.cores.max", "100")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "private_uid", "keyspace" -> "groups"))
      .load()

    val df2 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_allvalues", "keyspace" -> "uimg"))
      .load()

//    val df3 = df1.join(df2, "uid").select("uid",
//      "loc","recent_t1","recent_t2","recent_t3","last_t1","last_t2","last_t3",
//      "iphone","umt","applist","vid_t1","vid_t2","vid_t3","slide_t1","slide_t2","slide_t3",
//      "ustore","general_search","uts")

    val df3 = df1.join(df2, "uid").select("uid",
      "t1","t2","t3")

    df3.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "private_shede_groups", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()
  }
}
