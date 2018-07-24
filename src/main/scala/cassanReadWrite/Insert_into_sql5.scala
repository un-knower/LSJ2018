package cassanReadWrite

import libsvm.PackageChecker
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession, hive}

object Insert_into_sql5 {
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
/*        val df1 = spark
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "oppofindx_group", "keyspace" -> "groups"))
          .load()
          .filter("type='vivonex' or type='HuaweiP' or type='HuaweiMate'")
          .select($"uid"
            ,expr("1").alias("z10008")
          )

    df1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()*/

    import spark.sql
    val query1 = sql("select * from pagekv limit 10")
    val query2 = query1.map{ case Row(key:String, value:String) => s"Key: $key, Value: json_extract(json_parse($value),'category')"}
    query2.show(10)

  }


}
