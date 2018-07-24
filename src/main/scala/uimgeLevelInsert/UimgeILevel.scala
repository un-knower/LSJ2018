//package uimgeLevelInsert
//
//import libsvm.PackageChecker
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.SparkConf
//
//object UimgeILevel {
//  def main(args: Array[String]): Unit = {
//
//    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
//      "org.apache.spark:spark-sql_2.11:2.2.0",
//      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
//    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
//    val alljars = jars.split(",")
//
//    val conf = new SparkConf()
//      .setMaster("spark://10.90.9.111:7077")
//      .setAppName("Uimge_ALevel_insert_test")
//      .set("spark.cassandra.connection.host", "10.80.17.155")
//      .set("spark.executor.memory", "30g")
//      .set("spark.cores.max", "300")
//      .setJars(alljars)
//
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val df1 = spark.read.format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "test5", "keyspace" -> "uimg")).load()
//      .select("uid", "t1")
//      .map(all => (all(0), splitString(all(1).toString)))
//
//    df1.write
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "test2", "keyspace" -> "uimg"))
//      .mode(SaveMode.Append)
//      .save()
//  }
//
//  def splitString(str: String): Unit = {
//    val s = str.split("#")
//    s.map(s1 => {
//      val x = s1.split("_")
//      if (x.length == 4)
//        x(0)+x(3)
//      })
//  }
//
//}
//
