package libsvm

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}
//import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}

object Insert_into_sql {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("Uimge_DLevel_insert_into_manualapp2")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "30g")
      .set("spark.cores.max", "280")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_rnd_level", "keyspace" -> "groups"))
      .load()
//      .select($"uid",$"d101",$"d102",$"d103",$"d104",$"d105",$"d106"
//        ,$"d201",$"d202"
//        ,$"d301",$"d302",$"d303",$"d304",$"d305"
//        ,$"d401",$"d402",$"d403",$"d404",$"d405",$"d406",$"d407"
//        ,$"d501",$"d502",$"d503",$"d504"
//        ,expr("0").alias("d107")
//        ,expr("0").alias("d203")
//        ,expr("0").alias("d306")
//        ,expr("0").alias("d505"))

    df1.createOrReplaceTempView("table")
    val query1 = spark.sql("select uid," +
      "c101 as b101," +
      "c102 as b102," +
      "c103 as b103," +
      "c104 as b104," +
      "c105 as b105," +
      "c106 as b106," +
      "c201 as b201," +
      "c202 as b202," +
      "c301 as b301," +
      "c302 as b302," +
      "c303 as b303," +
      "c304 as b304," +
      "c305 as b305," +
      "c401 as b401," +
      "c402 as b402," +
      "c403 as b403," +
      "c404 as b404," +
      "c405 as b405," +
      "c406 as b406," +
      "c407 as b407," +
      "c501 as b501," +
      "c502 as b502," +
      "c503 as b503," +
      "c504 as b504 from table")

    query1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()
  }
}
